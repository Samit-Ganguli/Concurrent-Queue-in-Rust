use std::sync::atomic::Ordering;

#[cfg(any(target_arch = "x64",
          target_arch = "x64_64",
          target_arch = "aarch64",
          target_arch = "arm"))]
mod can_consume {
    use std::sync::atomic::Ordering;
    pub const CONSUME: Ordering = Ordering::Relaxed;
}

#[cfg(not(any(target_arch = "x64",
              target_arch = "x64_64",
              target_arch = "aarch64",
              target_arch = "arm")))]
mod can_consume {
    use std::sync::atomic::Ordering;
    pub const CONSUME: Ordering = Ordering::Acquire;
}

pub const CONSUME: Ordering = can_consume::CONSUME;
