use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStepPlan {
    pub id: String,
    pub status: String,
    pub description: String,
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_command: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelinePlan {
    pub created_unix_secs: u64,
    pub airports: Vec<String>,
    pub jobs_file: String,
    pub download_manifest_file: String,
    pub steps: Vec<PipelineStepPlan>,
}
