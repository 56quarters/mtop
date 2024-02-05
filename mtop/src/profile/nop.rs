use mtop_client::MtopError;

#[derive(Debug, Default)]
pub struct Profiler;

impl Profiler {
    pub fn new() -> Self {
        Self
    }

    pub fn proto(&self) -> Result<Vec<u8>, MtopError> {
        Ok(Vec::new())
    }
}
