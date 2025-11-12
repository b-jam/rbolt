use std::{fs::File, io::{self, Write, Seek}, path::Path, sync::RwLock};
use memmap2::{MmapMut, MmapOptions};

const PAGE_SIZE: usize = 4096;
const MAGIC: u32 = 0x73796E63;
const VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct Header {
    magic: u32,
    version: u32,
    page_size: u32,

    root_page_id: u64, // Location of Root Page
    free_list_page_id: u64, //Location of the Free List Page

    // Metadata
    highest_page_id: u64, //highest allocated page ID
    tx_id: u64, //transaction id
}

impl Header {
    /// Creates a default header for a new database.
    fn new(page_size: u32) -> Self {
        Header {
            magic: MAGIC,
            version: VERSION,
            page_size,
            root_page_id: 0,
            free_list_page_id: 1, // Free list on page 1
            highest_page_id: 2,   // Highest allocated page ID - start at 2
            tx_id: 0,
        }
    }
}

pub struct Db {
    mmap: RwLock<MmapMut>, //Immutable view of file. concurrent reads and exclusive write.
    header: Header,
    file: File, // File used to create mmap
}

impl Db {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create if it doesn't exist
            .open(path)?;

        let file_len = file.metadata()?.len() as usize;

        if file_len < PAGE_SIZE * 2 {
            // New database: initialize with minimim size
            file.set_len(PAGE_SIZE as u64 * 2)?;

            let default_header = Header::new(PAGE_SIZE as u32);
            Self::write_header(&mut file, &default_header)?;
        }

        let initial_mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .expect("Failed to create mutable memory map")
        };

        let header = Self::read_header(&initial_mmap)?;

        if header.magic != MAGIC {
            // TODO: Handle corruption or invalid format here.
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid database file magic num"));
        }

        Ok(Db {
            mmap: RwLock::new(initial_mmap),
            header,
            file,
        })

    }

    /// Writes the header bytes to the beginning of the file using a standard Write operation.
    /// This is used only for initial file creation.
    fn write_header(file: &mut File, header: &Header) -> io::Result<()> {

        // The header is written to the first page (Page 0)
        let header_bytes = unsafe {
            // Get a byte slice of the Header struct's memory
            std::slice::from_raw_parts(
                (header as *const Header) as *const u8,
                std::mem::size_of::<Header>(),
            )
        };

        // Rewind the file pointer to the start and write the header.
        file.seek(io::SeekFrom::Start(0))?;
        let mut writer = io::BufWriter::new(file);
        writer.write_all(header_bytes)?;
        writer.flush()?;

        // For initial setup, we don't need to sync the Mmap yet,
        // as the initial_mmap is created right after this.
        Ok(())
    }

    fn read_header(mmap: &MmapMut) -> io::Result<Header> {
        if mmap.len() < std::mem::size_of::<Header>() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "File too small to contain header"));
        }
        let header_ref = unsafe {
            let ptr = mmap.as_ptr() as *const Header;
            &*ptr
        };
        Ok(*header_ref)
    }

    pub fn begin_read_transaction(&self) -> io::Result<()> {
        // Acquire a read lock on the Mmap. This ensures no write transaction
        // is currently trying to update the Mmap reference.
        let current_mmap = self.mmap.read().unwrap();

        // At this point, 'current_mmap' is the immutable snapshot used by the transaction.
        // It remains valid even if the Db object's state changes later.

        println!("Read transaction started on database of size {} bytes.", current_mmap.len());
        // ... return a ReadTxn struct here ...

        Ok(())
    }

    pub fn commit_write_transaction(&self, new_data: &[u8]) -> io::Result<()> {
        // Acquite an exclusive write lock on Mmap. This blocks all new readers
        // and any currently waiting writers.
        let mut mmap_guard = self.mmap.write().unwrap();

        // 1. Update the file length and write new data
        // TODO: ually written to a temp buffer first).
        self.file.set_len(new_data.len() as u64)?;

        // 2. Re-map the file to include the new size and pages.
        // This operation replaces the old Mmap with a new one.
        let new_mmap = unsafe { MmapMut::map_mut(&self.file)? };

        // 3. Replace the protected internal Mmap.
        *mmap_guard = new_mmap;

        mmap_guard.flush()?;

        // The lock is released here, and new read transactions will now see the new data.
        println!("Write transaction committed and Mmap updated and flushed.");
        Ok(())

    }


}

fn main() -> io::Result<()> {
    let db_path = Path::new("test_db.rdb");

    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    println!("--- Opening/Initializing Database ---");
    let db = Db::open(db_path)?;

    println!("--- Starting Read Transaction ---");
    db.begin_read_transaction()?;

    println!("--- Committing Fake Write Transaction ---");
    // Simulate committing new data (e.g., 4KB * 3 pages)
    let fake_data = vec![0u8; PAGE_SIZE * 3];
    db.commit_write_transaction(&fake_data)?;

    // Clean up
    //std::fs::remove_file(db_path)?;

    Ok(())
}