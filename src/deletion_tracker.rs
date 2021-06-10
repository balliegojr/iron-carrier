use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use tokio::io::AsyncWriteExt;

pub(crate) struct DeletionTracker {
    log_path: PathBuf,
}

const FOURTEEN_DAYS_AS_SECS: u64 = 7 * 24 * 60 * 60;
#[cfg(not(windows))]
const LINE_ENDING: &str = "\n";
#[cfg(windows)]
const LINE_ENDING: &str = "\r\n";

impl DeletionTracker {
    pub fn new(alias_root_path: &Path) -> Self {
        DeletionTracker {
            log_path: alias_root_path.join(".ironcarrier"),
        }
    }

    pub fn get_files(&self) -> crate::Result<HashMap<PathBuf, SystemTime>> {
        if !self.log_path.exists() {
            log::debug!("deletion log doesn't exist");
            return Ok(HashMap::new());
        }

        log::debug!("reading deletion log content");
        let contents = std::fs::read(&self.log_path)?;
        let contents = std::str::from_utf8(&contents)?;

        log::debug!("parsing deletiong log");
        let log_entries = self.parse_log(&contents);
        return self.clean_and_rewrite(log_entries);
    }

    fn clean_and_rewrite(
        &self,
        mut log_entries: HashMap<PathBuf, SystemTime>,
    ) -> crate::Result<HashMap<PathBuf, SystemTime>> {
        let count_before = log_entries.len();

        let limit_date = SystemTime::now() - Duration::from_secs(FOURTEEN_DAYS_AS_SECS);
        log_entries.retain(|_, v| *v >= limit_date);

        if log_entries.len() == 0 {
            log::debug!(
                "dropped {} entries from log file, removing log file",
                count_before
            );
            std::fs::remove_file(&self.log_path)?;
        } else if count_before != log_entries.len() {
            log::debug!(
                "dropping {} entries from log file, recreating log",
                count_before - log_entries.len()
            );
            let mut log_file = std::fs::File::create(&self.log_path)?;

            // TODO: use buffered writer
            for (path, time) in &log_entries {
                log_file.write_all(&self.create_line(&path, &time).as_bytes())?;
            }
            log_file.flush()?;
        }

        Ok(log_entries)
    }

    fn create_line(&self, path: &Path, time: &SystemTime) -> String {
        let time = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        format!("{},{}{}", path.display(), time.as_secs(), LINE_ENDING)
    }

    fn parse_line(&self, log_line: &str) -> Option<(PathBuf, SystemTime)> {
        let mut line = log_line.split(',');
        let d_path = line.next()?;
        let d_time: u64 = line.next().and_then(|v| v.parse::<u64>().ok())?;

        Some((
            PathBuf::from(d_path),
            SystemTime::UNIX_EPOCH + Duration::from_secs(d_time),
        ))
    }

    fn parse_log(&self, log_content: &str) -> HashMap<PathBuf, SystemTime> {
        log_content
            .lines()
            .filter_map(|line| self.parse_line(line))
            .collect()
    }

    pub async fn add_entry(&self, path: &Path) -> crate::Result<()> {
        log::debug!("adding entry to log file: {:?}", path);
        let mut log_file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)
            .await?;

        log_file
            .write_all(&self.create_line(path, &SystemTime::now()).as_bytes())
            .await?;

        log_file.flush().await?;

        Ok(())
    }

    pub async fn remove_entry(&self, path: &Path) -> crate::Result<()> {
        if !self.log_path.exists() {
            return Ok(());
        }

        log::debug!("removing entry {:?} from log", path);
        let mut log_file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&self.log_path)
            .await?;

        log_file
            .write_all(&self.create_line(path, &SystemTime::UNIX_EPOCH).as_bytes())
            .await?;

        log_file.flush().await?;

        Ok(())
    }
}
