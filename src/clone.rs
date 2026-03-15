use std::cell::RefCell;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::Instant;

use anyhow::{Result, bail};
#[cfg(not(windows))]
use signal_hook::consts::{SIGHUP, SIGQUIT};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};

use crate::{
    commit::Commit,
    data::object::{GIT_DIR, GIT_HEAD_FILE, GIT_OBJECTS_DIR, GIT_REFS_DIR, ObjectStore},
    data::tree::Tree,
    error::{GitError, GitResult},
    pack::index::index_pack,
    remote::RemoteClient,
};

pub struct Clone {
    repo_url: String,
    target_dir: PathBuf,
}

impl Clone {
    pub fn run(repo_url: &str, target_dir: Option<PathBuf>) -> GitResult<()> {
        Self::new(repo_url, target_dir)?.execute()
    }

    fn new(repo_url: &str, target_dir: Option<PathBuf>) -> GitResult<Self> {
        let repo_url = repo_url.to_string();
        let target_dir = match target_dir {
            Some(target_dir) => target_dir,
            None => {
                let repo_name = repo_url
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .map(|segment| segment.strip_suffix(".git").unwrap_or(segment))
                    .filter(|segment| !segment.is_empty())
                    .ok_or(GitError::CantGuessCloneTarget)?;
                PathBuf::from(repo_name)
            }
        };

        Ok(Self {
            repo_url,
            target_dir,
        })
    }

    fn execute(&self) -> GitResult<()> {
        let progress = RefCell::new(ProgressRenderer::default());
        self.ensure_empty_target()?;
        Self::install_signal_handler()?;
        Self::reset_interrupt_state();

        let result = {
            let mut target_dir_guard = CloneTargetDir::new(&self.target_dir)?;
            (|| -> GitResult<()> {
                progress
                    .borrow_mut()
                    .print_line(&format!("Cloning into '{}'...", self.target_dir.display()));

                let git_dir = Self::init_repo_layout(target_dir_guard.path())?;
                let store = ObjectStore::new(git_dir.clone());

                let remote = RemoteClient::new(&self.repo_url)?;
                let discovery = remote.discover()?;
                Self::check_interrupt()?;
                progress.borrow_mut().start_receiving();
                let fetch_started = Instant::now();
                Self::set_disconnect_message_active(true);
                let parsed_pack = remote.fetch_packfile(
                    &store.pack_dir(),
                    &discovery.head_hash,
                    &discovery.capabilities,
                    |msg| {
                        Self::check_interrupt()?;
                        progress.borrow_mut().remote_chunk(msg);
                        Ok(())
                    },
                    |pack_bytes, total_objects, received_objects| {
                        Self::check_interrupt()?;
                        Self::set_disconnect_message_active(false);
                        progress.borrow_mut().update_pack_progress(
                            pack_bytes,
                            total_objects,
                            received_objects,
                        );
                        Ok(())
                    },
                );
                Self::set_disconnect_message_active(false);
                let parsed_pack = parsed_pack?;
                Self::check_interrupt()?;
                let fetch_elapsed = fetch_started.elapsed();
                progress.borrow_mut().finish_remote_output();
                progress.borrow_mut().finish_receiving(fetch_elapsed);

                let unpack_started = Instant::now();
                let stats = index_pack(
                    &store,
                    &parsed_pack,
                    |progress_update| {
                        Self::check_interrupt()?;
                        progress.borrow_mut().resolving_update(
                            progress_update.resolved_deltas,
                            progress_update.total_deltas,
                        );
                        Ok(())
                    },
                    Self::check_interrupt,
                )?;
                let unpack_elapsed = unpack_started.elapsed();

                Self::write_refs(&git_dir, &discovery.head_ref, &discovery.head_hash)?;

                let root_tree = Commit::root_tree_in(&store, &discovery.head_hash)?;
                Self::check_interrupt()?;
                progress
                    .borrow_mut()
                    .finish_resolving(stats.deltas, unpack_elapsed);

                let total_files = Tree::count_checkout_items_in(&store, &root_tree)?;
                Self::check_interrupt()?;
                Tree::checkout_in_with_progress(
                    &store,
                    &root_tree,
                    target_dir_guard.path(),
                    &mut |updated_files| {
                        Self::check_interrupt()?;
                        progress
                            .borrow_mut()
                            .updating_files_update(updated_files, total_files);
                        Ok(())
                    },
                )?;
                progress.borrow_mut().finish_updating_files(total_files);
                target_dir_guard.finish();

                Ok(())
            })()
        };

        if let Some(interrupt) = Self::take_pending_interrupt() {
            if interrupt.emit_disconnect_message {
                eprintln!("fetch-pack: unexpected disconnect while reading sideband packets");
            }
            process::exit(128 + interrupt.signal);
        }

        Self::reset_interrupt_state();
        result
    }

    fn ensure_empty_target(&self) -> GitResult<()> {
        if !self.target_dir.exists() {
            return Ok(());
        }

        let metadata = fs::metadata(&self.target_dir)?;
        if !metadata.is_dir() {
            return Err(GitError::CloneTargetNotEmpty(self.target_dir.clone()));
        }

        if fs::read_dir(&self.target_dir)?.next().is_some() {
            return Err(GitError::CloneTargetNotEmpty(self.target_dir.clone()));
        }

        Ok(())
    }

    fn init_repo_layout(target_dir: &Path) -> GitResult<PathBuf> {
        let git_dir = target_dir.join(GIT_DIR);
        fs::create_dir_all(git_dir.join(GIT_OBJECTS_DIR))?;
        fs::create_dir_all(git_dir.join(GIT_REFS_DIR))?;
        Ok(git_dir)
    }

    fn write_refs(git_dir: &Path, head_ref: &str, head_hash: &str) -> GitResult<()> {
        fs::write(git_dir.join(GIT_HEAD_FILE), format!("ref: {head_ref}\n"))?;
        let ref_path = git_dir.join(head_ref);
        if let Some(parent) = ref_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(ref_path, format!("{head_hash}\n"))?;
        Ok(())
    }

    fn install_signal_handler() -> GitResult<()> {
        static HANDLER_INSTALLED: OnceLock<()> = OnceLock::new();
        if HANDLER_INSTALLED.get().is_some() {
            return Ok(());
        }

        let mut signals = Signals::new(Self::signal_cleanup_signals())?;
        thread::spawn(move || {
            if let Some(signal) = signals.forever().next() {
                let state = Clone::signal_state();
                if state
                    .interrupted
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    state.signal.store(signal, Ordering::SeqCst);
                    state.emit_disconnect_message.store(
                        state.disconnect_message_active.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                }
            }
        });

        let _ = HANDLER_INSTALLED.set(());
        Ok(())
    }

    #[cfg(windows)]
    fn signal_cleanup_signals() -> &'static [i32] {
        &[SIGINT, SIGTERM]
    }

    #[cfg(not(windows))]
    fn signal_cleanup_signals() -> &'static [i32] {
        &[SIGHUP, SIGINT, SIGQUIT, SIGTERM]
    }

    fn signal_state() -> &'static CloneSignalState {
        static STATE: CloneSignalState = CloneSignalState::new();
        &STATE
    }

    fn reset_interrupt_state() {
        let state = Self::signal_state();
        state.interrupted.store(false, Ordering::SeqCst);
        state.signal.store(0, Ordering::SeqCst);
        state.emit_disconnect_message.store(false, Ordering::SeqCst);
        state
            .disconnect_message_active
            .store(false, Ordering::SeqCst);
    }

    fn set_disconnect_message_active(active: bool) {
        Self::signal_state()
            .disconnect_message_active
            .store(active, Ordering::SeqCst);
    }

    fn take_pending_interrupt() -> Option<CloneInterrupt> {
        let state = Self::signal_state();
        if state.interrupted.swap(false, Ordering::SeqCst) {
            return Some(CloneInterrupt {
                signal: state.signal.swap(0, Ordering::SeqCst),
                emit_disconnect_message: state
                    .emit_disconnect_message
                    .swap(false, Ordering::SeqCst),
            });
        }
        None
    }

    fn check_interrupt() -> Result<()> {
        if Self::signal_state().interrupted.load(Ordering::SeqCst) {
            bail!("clone interrupted")
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct CloneInterrupt {
    signal: i32,
    emit_disconnect_message: bool,
}

struct CloneSignalState {
    interrupted: AtomicBool,
    signal: AtomicI32,
    emit_disconnect_message: AtomicBool,
    disconnect_message_active: AtomicBool,
}

impl CloneSignalState {
    const fn new() -> Self {
        Self {
            interrupted: AtomicBool::new(false),
            signal: AtomicI32::new(0),
            emit_disconnect_message: AtomicBool::new(false),
            disconnect_message_active: AtomicBool::new(false),
        }
    }
}

struct CloneTargetDir {
    path: PathBuf,
    existed_before: bool,
    active: bool,
}

impl CloneTargetDir {
    fn new(target_dir: &Path) -> GitResult<Self> {
        let path = Self::absolute_path(target_dir)?;
        let existed_before = path.exists();
        if !existed_before {
            fs::create_dir_all(&path)?;
        }

        Ok(Self {
            path,
            existed_before,
            active: true,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn finish(&mut self) {
        self.active = false;
    }

    fn remove_contents(&self) -> GitResult<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(path)?;
            } else {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    fn absolute_path(path: &Path) -> GitResult<PathBuf> {
        if path.is_absolute() {
            return Ok(path.to_path_buf());
        }
        Ok(std::env::current_dir()?.join(path))
    }
}

impl Drop for CloneTargetDir {
    fn drop(&mut self) {
        if !self.active || !self.path.exists() {
            return;
        }

        if self.existed_before {
            let _ = self.remove_contents();
        } else {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Default)]
struct ProgressRenderer {
    remote_buffer: String,
    active_line: bool,
    receiving_started: Option<Instant>,
    last_pack_bytes: usize,
    total_objects: Option<usize>,
    last_received_objects: usize,
    last_received_percent: Option<usize>,
    last_resolved_deltas: usize,
    last_resolved_percent: Option<usize>,
    last_updated_files: usize,
    last_updated_percent: Option<usize>,
}

impl ProgressRenderer {
    fn remote_chunk(&mut self, chunk: &str) {
        self.remote_buffer.push_str(chunk);

        while let Some(idx) = self.remote_buffer.find(['\r', '\n']) {
            let line = self.remote_buffer[..idx].to_string();
            let delimiter = self.remote_buffer.as_bytes()[idx] as char;
            self.remote_buffer.drain(..=idx);

            if line.is_empty() {
                continue;
            }

            let line = if line.starts_with("remote:") {
                line
            } else {
                format!("remote: {line}")
            };
            self.update_total_objects(&line);

            match delimiter {
                '\r' => self.print_status(&line),
                '\n' => {
                    self.print_line(&line);
                    self.maybe_render_receiving_status();
                }
                _ => {}
            }
        }
    }

    fn finish_remote_output(&mut self) {
        if !self.remote_buffer.is_empty() {
            let mut line = std::mem::take(&mut self.remote_buffer);
            if !line.starts_with("remote:") {
                line = format!("remote: {line}");
            }
            self.update_total_objects(&line);
            self.print_line(&line);
        }

        if self.active_line || !self.remote_buffer.is_empty() {
            self.maybe_render_receiving_status();
        }
    }

    fn receiving_update(&mut self, received_objects: usize, total_objects: usize) {
        let object_percent = Self::percent(received_objects, total_objects);
        self.last_received_objects = received_objects;
        if self.last_received_percent == Some(object_percent) {
            return;
        }
        if object_percent >= 100 {
            self.last_received_percent = Some(100);
            return;
        }

        self.print_status(&self.receiving_status(object_percent, received_objects, total_objects));
        self.last_received_percent = Some(object_percent);
    }

    fn start_receiving(&mut self) {
        self.receiving_started = Some(Instant::now());
    }

    fn update_pack_progress(
        &mut self,
        pack_bytes: usize,
        total_objects: Option<usize>,
        received_objects: usize,
    ) {
        if self.total_objects.is_none() {
            self.total_objects = total_objects;
        }
        self.last_pack_bytes = pack_bytes;
        if let Some(total_objects) = self.total_objects {
            self.receiving_update(received_objects, total_objects);
        }
    }

    fn maybe_render_receiving_status(&mut self) {
        let Some(total_objects) = self.total_objects else {
            return;
        };
        if self.last_pack_bytes == 0 && self.last_received_objects == 0 {
            return;
        }
        if !self.remote_buffer.is_empty() {
            return;
        }
        if self.last_received_percent == Some(100) {
            return;
        }

        self.print_status(&self.receiving_status(
            Self::percent(self.last_received_objects, total_objects),
            self.last_received_objects,
            total_objects,
        ));
    }

    fn finish_receiving(&mut self, elapsed: std::time::Duration) {
        let total_objects = self.total_objects.unwrap_or(self.last_received_objects);
        let bytes = self.last_pack_bytes;
        self.print_line(&format!(
            "Receiving objects: 100% ({0}/{0}), {1} | {2}/s, done.",
            total_objects,
            Self::human_size(bytes),
            Self::human_size(Self::rate_bytes_per_second(bytes, elapsed))
        ));
        self.last_pack_bytes = bytes;
        self.last_received_objects = total_objects;
        self.last_received_percent = Some(100);
    }

    fn finish_resolving(&mut self, deltas: usize, elapsed: std::time::Duration) {
        if deltas == 0 {
            return;
        }

        self.print_line(&format!(
            "Resolving deltas: 100% ({0}/{0}), done in {1}.",
            deltas,
            Self::human_duration(elapsed)
        ));
    }

    fn resolving_update(&mut self, resolved_deltas: usize, total_deltas: usize) {
        if total_deltas == 0 {
            return;
        }

        let delta_percent = Self::percent(resolved_deltas, total_deltas);
        let stride = Self::progress_update_stride(total_deltas);
        let progressed = resolved_deltas.saturating_sub(self.last_resolved_deltas);
        if self.last_resolved_percent == Some(delta_percent) && progressed < stride {
            return;
        }

        self.print_status(&format!(
            "Resolving deltas: {:>3}% ({}/{})",
            delta_percent, resolved_deltas, total_deltas
        ));
        self.last_resolved_deltas = resolved_deltas;
        self.last_resolved_percent = Some(delta_percent);
    }

    fn updating_files_update(&mut self, updated_files: usize, total_files: usize) {
        if total_files == 0 {
            return;
        }

        let file_percent = Self::percent(updated_files, total_files);
        let stride = Self::progress_update_stride(total_files);
        let progressed = updated_files.saturating_sub(self.last_updated_files);
        if self.last_updated_percent == Some(file_percent) && progressed < stride {
            return;
        }

        self.print_status(&format!(
            "Updating files: {:>3}% ({}/{})",
            file_percent, updated_files, total_files
        ));
        self.last_updated_files = updated_files;
        self.last_updated_percent = Some(file_percent);
    }

    fn finish_updating_files(&mut self, total_files: usize) {
        self.print_line(&format!(
            "Updating files: 100% ({0}/{0}), done.",
            total_files
        ));
    }

    fn print_status(&mut self, line: &str) {
        eprint!("\r\x1b[2K{line}");
        let _ = io::stderr().flush();
        self.active_line = true;
    }

    fn print_line(&mut self, line: &str) {
        eprintln!("\r\x1b[2K{line}");
        self.active_line = false;
    }

    fn receiving_status(
        &self,
        object_percent: usize,
        received_objects: usize,
        total_objects: usize,
    ) -> String {
        if let Some(started) = self.receiving_started {
            let elapsed = started.elapsed();
            return format!(
                "Receiving objects: {:>3}% ({}/{}), {} | {}/s",
                object_percent,
                received_objects,
                total_objects,
                Self::human_size(self.last_pack_bytes),
                Self::human_size(Self::rate_bytes_per_second(self.last_pack_bytes, elapsed))
            );
        }

        format!(
            "Receiving objects: {:>3}% ({}/{}),",
            object_percent, received_objects, total_objects
        )
    }

    fn update_total_objects(&mut self, line: &str) {
        if self.total_objects.is_some() {
            return;
        }

        let Some(total) = line.strip_prefix("remote: Total ") else {
            return;
        };
        let Some((count, _)) = total.split_once(' ') else {
            return;
        };
        if let Ok(count) = count.parse() {
            self.total_objects = Some(count);
        }
    }

    fn percent(current: usize, total: usize) -> usize {
        if total == 0 {
            return 100;
        }
        (current * 100) / total
    }

    fn progress_update_stride(total: usize) -> usize {
        (total / 100).max(1)
    }

    fn rate_bytes_per_second(bytes: usize, elapsed: std::time::Duration) -> usize {
        let seconds = elapsed.as_secs_f64();
        if seconds == 0.0 {
            return bytes;
        }
        (bytes as f64 / seconds).round() as usize
    }

    fn human_size(bytes: usize) -> String {
        const KIB: f64 = 1024.0;
        const MIB: f64 = 1024.0 * 1024.0;

        let bytes = bytes as f64;
        if bytes >= MIB {
            format!("{:.2} MiB", bytes / MIB)
        } else if bytes >= KIB {
            format!("{:.2} KiB", bytes / KIB)
        } else {
            format!("{:.0} B", bytes)
        }
    }

    fn human_duration(elapsed: std::time::Duration) -> String {
        if elapsed.as_millis() == 0 {
            return "<1ms".to_string();
        }
        if elapsed.as_secs() > 0 {
            format!("{:.2}s", elapsed.as_secs_f64())
        } else {
            format!("{}ms", elapsed.as_millis())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn human_size_formats_binary_units() {
        assert_eq!(ProgressRenderer::human_size(512), "512 B");
        assert_eq!(ProgressRenderer::human_size(1024), "1.00 KiB");
        assert_eq!(ProgressRenderer::human_size(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn empty_clone_target_rejects_non_empty_directory() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("README.md"), b"hello").unwrap();

        let clone = Clone::new(
            "https://github.com/example/repo.git",
            Some(temp.path().into()),
        )
        .unwrap();
        let error = clone.ensure_empty_target().unwrap_err();
        assert!(matches!(error, GitError::CloneTargetNotEmpty(_)));
    }

    #[test]
    fn percent_handles_zero_total() {
        assert_eq!(ProgressRenderer::percent(0, 0), 100);
        assert_eq!(ProgressRenderer::percent(1, 4), 25);
    }

    #[test]
    fn remote_chunk_buffers_partial_lines() {
        let mut renderer = ProgressRenderer::default();
        renderer.remote_chunk("Counting objects:  10%");
        assert_eq!(renderer.remote_buffer, "Counting objects:  10%");
    }

    #[test]
    fn receiving_progress_starts_when_pack_header_exposes_object_count() {
        let mut renderer = ProgressRenderer::default();

        renderer.start_receiving();
        renderer.update_pack_progress(8 * 1024, Some(245), 0);

        assert_eq!(renderer.total_objects, Some(245));
        assert_eq!(renderer.last_pack_bytes, 8 * 1024);
        assert!(renderer.active_line);
        assert_eq!(renderer.last_received_percent, Some(0));
    }

    #[test]
    fn receiving_progress_waits_for_object_count() {
        let mut renderer = ProgressRenderer::default();

        renderer.start_receiving();
        renderer.update_pack_progress(8 * 1024, None, 0);
        assert_eq!(renderer.last_received_percent, None);

        renderer.update_pack_progress(16 * 1024, Some(245), 0);

        assert_eq!(renderer.total_objects, Some(245));
        assert_eq!(renderer.last_pack_bytes, 16 * 1024);
        assert!(renderer.active_line);
        assert_eq!(renderer.last_received_percent, Some(0));
    }

    #[test]
    fn receiving_progress_only_rerenders_on_percent_change() {
        let mut renderer = ProgressRenderer::default();

        renderer.start_receiving();
        renderer.update_pack_progress(32 * 1024 * 1024, Some(1_000_000), 512);
        assert_eq!(renderer.last_received_objects, 512);
        assert_eq!(renderer.last_received_percent, Some(0));

        renderer.update_pack_progress(32 * 1024 * 1024, Some(1_000_000), 700);
        assert_eq!(renderer.last_received_objects, 700);
        assert_eq!(renderer.last_received_percent, Some(0));

        renderer.update_pack_progress(32 * 1024 * 1024, Some(1_000_000), 10_000);

        assert_eq!(renderer.last_received_percent, Some(1));
        assert_eq!(renderer.last_received_objects, 10_000);
        assert!(renderer.active_line);
    }

    #[test]
    fn receiving_progress_does_not_render_transient_hundred_percent_line() {
        let mut renderer = ProgressRenderer::default();

        renderer.start_receiving();
        renderer.update_pack_progress(32 * 1024 * 1024, Some(100), 100);

        assert_eq!(renderer.last_received_percent, Some(100));
        assert!(!renderer.active_line);
    }

    #[test]
    fn finish_remote_output_preserves_active_receiving_line() {
        let mut renderer = ProgressRenderer::default();

        renderer.start_receiving();
        renderer.update_pack_progress(32 * 1024 * 1024, Some(100), 99);
        assert!(renderer.active_line);
        assert_eq!(renderer.last_received_percent, Some(99));

        renderer.finish_remote_output();

        assert!(renderer.active_line);
        assert_eq!(renderer.last_received_percent, Some(99));
    }

    #[test]
    fn clone_signal_state_tracks_disconnect_message_separately() {
        Clone::reset_interrupt_state();
        Clone::set_disconnect_message_active(true);
        let state = Clone::signal_state();
        assert!(!state.interrupted.load(Ordering::SeqCst));
        assert!(state.disconnect_message_active.load(Ordering::SeqCst));

        Clone::set_disconnect_message_active(false);
        assert!(!state.interrupted.load(Ordering::SeqCst));
        assert!(!state.disconnect_message_active.load(Ordering::SeqCst));

        Clone::reset_interrupt_state();
    }

    #[test]
    fn resolving_progress_throttles_within_same_percent() {
        let mut renderer = ProgressRenderer::default();

        renderer.resolving_update(1, 10_000);
        assert_eq!(renderer.last_resolved_deltas, 1);

        renderer.resolving_update(50, 10_000);
        assert_eq!(renderer.last_resolved_deltas, 1);

        renderer.resolving_update(100, 10_000);
        assert_eq!(renderer.last_resolved_deltas, 100);
        assert_eq!(renderer.last_resolved_percent, Some(1));
    }

    #[test]
    fn updating_files_progress_throttles_within_same_percent() {
        let mut renderer = ProgressRenderer::default();

        renderer.updating_files_update(1, 1_000);
        assert_eq!(renderer.last_updated_files, 1);

        renderer.updating_files_update(5, 1_000);
        assert_eq!(renderer.last_updated_files, 1);

        renderer.updating_files_update(10, 1_000);
        assert_eq!(renderer.last_updated_files, 10);
        assert_eq!(renderer.last_updated_percent, Some(1));
    }

    #[test]
    fn absolute_path_resolves_relative_paths_from_cwd() {
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(
            CloneTargetDir::absolute_path(Path::new("repo")).unwrap(),
            cwd.join("repo")
        );

        let absolute = PathBuf::from("/tmp/repo");
        assert_eq!(CloneTargetDir::absolute_path(&absolute).unwrap(), absolute);
    }
}
