window.APP_CONFIG = {
  API_BASE: "api/v1",
  BACKEND_MODES: {
    local: {
      label: "Local",
      apiBase: "api/v1"
    },
    nifi: {
      label: "NiFi",
      apiBase: "api/v1"
    }
  },
  DEFAULT_BACKEND_MODE: "local",
  USE_MOCK_API: false,
  FEATURE_FLAGS: {
    jetlinksAlarmPush: false,
    jetlinksSso: false,
    jetlinksWorkbenchEntry: false,
    localTaskFlow: true,
    localFilePreview: true,
    localTagging: true
  }
};
