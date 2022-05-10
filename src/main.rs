use std::fs::OpenOptions;

use byteorder::{LittleEndian, ReadBytesExt};
use failure::Fallible;
use positioned_io::{Cursor, ReadAt, Size, Slice};

use custom_debug_derive::Debug as CustomDebug;

type Result<T> = std::result::Result<T, failure::Error>;

struct Reader<IO: ReadAt> {
    inner: IO,
}

impl<IO: ReadAt> Reader<IO> {
    fn new(io: IO) -> Self {
        Self { inner: io }
    }

    fn u8(&self, offset: u64) -> Fallible<u8> {
        let mut cursor = Cursor::new_pos(&self.inner, offset);
        Ok(cursor.read_u8()?)
    }

    fn u16(&self, offset: u64) -> Fallible<u16> {
        let mut cursor = Cursor::new_pos(&self.inner, offset);
        Ok(cursor.read_u16::<LittleEndian>()?)
    }

    fn u32(&self, offset: u64) -> Fallible<u32> {
        let mut cursor = Cursor::new_pos(&self.inner, offset);
        Ok(cursor.read_u32::<LittleEndian>()?)
    }

    fn u64_lohi(&self, lo_offset: u64, hi_offset: u64) -> Fallible<u64> {
        let lo = self.u32(lo_offset)?;
        let hi = self.u32(hi_offset)?;
        Ok((hi as u64) << 32 | lo as u64)
    }

    fn vec(&self, offset: u64, len: usize) -> Fallible<Vec<u8>> {
        let mut buff = vec![0u8; len];
        self.inner.read_exact_at(offset, &mut buff)?;
        Ok(buff)
    }
}

#[derive(CustomDebug)]
struct SuperBlock {
    #[debug(format = "0x{:X}")]
    magic: u16,
    block_size: u64,
    block_per_group: u64,
    inode_per_group: u64,
    inode_size: u64,
}

impl SuperBlock {
    fn new<T: ReadAt>(dev: T) -> Result<Self> {
        let r = Reader::new(Slice::new(dev, 1024, None));
        let magic = r.u16(0x38)?;
        let block_size = 2_u64.pow(10 + r.u32(0x18)?);
        let bpg = r.u32(0x20)?;
        let ipg = r.u32(0x28)?;
        let inode_size = r.u16(0x58)? as u64;
        Ok(Self {
            magic,
            block_size,
            block_per_group: bpg as _,
            inode_per_group: ipg as _,
            inode_size,
        })
    }
}

#[derive(Debug)]
struct BlockGroupDescriptor {
    inode_table: u64,
}

impl BlockGroupDescriptor {
    // every single descriptor takes 64 bytes
    const SIZE: u64 = 64;

    fn new<T: ReadAt>(slice: T) -> Result<Self> {
        let r = Reader::new(slice);
        Ok(Self {
            inode_table: r.u64_lohi(0x8, 0x28)?,
        })
    }
}

#[derive(Debug)]
struct BlockGroupNumber(u64);
impl BlockGroupNumber {
    fn block_group_descriptor_slice<T: ReadAt>(self, sb: &SuperBlock, dev: T) -> Slice<T> {
        // supper block takes 1 block
        let block_group_descriptor_start = sb.block_size;
        let offset = block_group_descriptor_start + self.0 * BlockGroupDescriptor::SIZE;
        Slice::new(dev, offset, None)
    }

    fn block_group_descriptor<T: ReadAt>(
        self,
        sb: &SuperBlock,
        dev: T,
    ) -> Result<BlockGroupDescriptor> {
        let slice = self.block_group_descriptor_slice(sb, dev);
        BlockGroupDescriptor::new(slice)
    }
}

#[derive(Copy, Clone, Debug)]
struct InodeNumber(u64);
impl InodeNumber {
    fn block_group_number(self, sb: &SuperBlock) -> BlockGroupNumber {
        let n = (self.0 - 1) / sb.inode_per_group;
        BlockGroupNumber(n)
    }

    fn inode_slice<T: ReadAt>(self, sb: &SuperBlock, dev: T) -> Result<Slice<T>> {
        let bgd = self
            .block_group_number(sb)
            .block_group_descriptor(sb, &dev)?;
        let inode_table_offset = bgd.inode_table * sb.block_size;
        let inode_index = (self.0 - 1) % sb.inode_per_group;
        let inode_offset = inode_table_offset + inode_index * sb.inode_size;
        Ok(Slice::new(dev, inode_offset, Some(sb.inode_size)))
    }

    fn inode(self, sb: &SuperBlock, dev: &dyn ReadAt) -> Result<Inode> {
        let slice = self.inode_slice(sb, dev)?;
        Inode::new(slice)
    }
}
#[derive(CustomDebug)]
struct Inode {
    #[debug(format = "{:o}")]
    mode: u16,
    size: u64,

    #[debug(skip)]
    block: Vec<u8>,
}

impl Inode {
    fn new<T: ReadAt>(slice: T) -> Result<Self> {
        let r = Reader::new(slice);
        Ok(Self {
            mode: r.u16(0x0)?,
            size: r.u64_lohi(0x4, 0x6C)?,
            block: r.vec(0x28, 60)?,
        })
    }

    fn file_type(&self) -> FileType {
        FileType::try_from(self.mode & 0xF000).unwrap()
    }

    fn data<T>(&self, sb: &SuperBlock, dev: T) -> Result<Slice<T>>
    where
        T: ReadAt,
    {
        let ext_header = ExtentHeader::new(&Slice::new(&self.block, 0, Some(12)))?;
        // assert_eq!(ext_header.depth, 0);
        // assert_eq!(ext_header.entries, 1);
        println!("{ext_header:?}");

        let ext = Extent::new(&Slice::new(&self.block, 12, Some(12)))?;
        assert_eq!(ext.len, 1);
        println!("{ext:?}");

        let offset = ext.start * sb.block_size;
        let len = ext.len * sb.block_size;
        Ok(Slice::new(dev, offset, Some(len)))
    }

    fn dir_entries(&self, sb: &SuperBlock, dev: &dyn ReadAt) -> Result<Vec<DirectoryEntry>> {
        let data = self.data(sb, dev)?;
        let total_len = data.size().expect("inode data need size").unwrap();

        let mut entries = Vec::new();
        let mut offset: u64 = 0;
        loop {
            if offset >= total_len {
                break;
            }
            let entry = DirectoryEntry::new(&Slice::new(&data, offset, None))?;
            offset += entry.len;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn find_entry_name(
        &self,
        sb: &SuperBlock,
        dev: &dyn ReadAt,
        name: &str,
    ) -> Result<Option<InodeNumber>> {
        let entries = self.dir_entries(sb, dev)?;
        Ok(entries
            .iter()
            .filter(|x| x.name == name)
            .map(|x| x.inode)
            .next())
    }
}

use num_enum::*;
use std::convert::TryFrom;

#[derive(Debug, TryFromPrimitive)]
#[repr(u16)]
enum FileType {
    Fifo = 0x1000,
    CharacterDevice = 0x2000,
    Directory = 0x4000,
    BlockDevice = 0x6000,
    Regular = 0x8000,
    SymbolicLink = 0xA000,
    Socket = 0xC000,
}

#[derive(Debug)]
struct ExtentHeader {
    entries: u64,
    depth: u64,
}

impl ExtentHeader {
    fn new<T: ReadAt>(slice: T) -> Result<Self> {
        let r = Reader::new(slice);
        let magic = r.u16(0x0)?;
        assert_eq!(magic, 0xF30A);

        Ok(Self {
            entries: r.u16(0x2)? as u64,
            depth: r.u16(0x6)? as u64,
        })
    }
}

#[derive(Debug)]
struct Extent {
    len: u64,
    start: u64,
}

impl Extent {
    fn new(slice: &dyn ReadAt) -> Result<Self> {
        let r = Reader::new(slice);
        Ok(Self {
            len: r.u16(0x4)? as u64,
            // the block number the extent points to is split
            // between upper 16-bits and lower 32-bits.
            start: ((r.u16(0x6)? as u64) << 32) + r.u32(0x8)? as u64,
        })
    }
}

#[derive(CustomDebug)]
struct DirectoryEntry {
    #[debug(skip)]
    len: u64,
    inode: InodeNumber,
    name: String,
}

impl DirectoryEntry {
    fn new(slice: &dyn ReadAt) -> Result<Self> {
        let r = Reader::new(slice);
        let name_len = r.u8(0x6)? as usize;
        Ok(Self {
            inode: InodeNumber(r.u32(0x0)? as u64),
            len: r.u16(0x4)? as u64,
            name: String::from_utf8_lossy(&r.vec(0x8, name_len)?).into(),
        })
    }
}

fn main() -> Result<()> {
    let file = OpenOptions::new().read(true).open("/dev/vdb1")?;
    let super_block = SuperBlock::new(&file)?;
    println!("{:#?}", super_block);

    // root `/` has fixed inode position 2
    let root_bg = InodeNumber(2).block_group_number(&super_block);
    println!("{:#?}", root_bg);
    let root_bgd = root_bg.block_group_descriptor_slice(&super_block, &file);
    let root_bgd = BlockGroupDescriptor::new(&root_bgd)?;
    println!("{root_bgd:#?}");

    let root_inode = InodeNumber(2).inode(&super_block, &file)?;
    // println!("{root_inode:#?} {:?}", root_inode.file_type());
    // let ext_header = ExtentHeader::new(Slice::new(&root_inode.block, 0, Some(12)))?;
    // println!("{ext_header:#?}");
    // let ext = Extent::new(&Slice::new(&root_inode.block, 12, Some(12)))?;
    // println!("{:#?}", ext);
    let dir_entries = root_inode.dir_entries(&super_block, &file)?;
    println!("{:#?}", dir_entries);

    let entry_name = "dind";
    let dind_inode = root_inode
        .find_entry_name(&super_block, &file, entry_name)?
        .expect("/data/dind should exist")
        .inode(&super_block, &file)?;
    println!("find inode(/data/dind): {dind_inode:?}");
    let run_sh_inode = dind_inode
        .find_entry_name(&super_block, &file, "run.sh")?
        .expect("/data/dind/run.sh should exists")
        .inode(&super_block, &file)?;
    println!(
        "find inode({:?})(/data/dind/run.sh): {run_sh_inode:?}",
        run_sh_inode.file_type()
    );
    let data = run_sh_inode.data(&super_block, &file)?;
    let mut buf = vec![0u8; run_sh_inode.size as usize];
    data.read_at(0, &mut buf)?;
    println!(
        "read run.sh({}):\n{}",
        run_sh_inode.size,
        String::from_utf8_lossy(&buf)
    );

    Ok(())
}
