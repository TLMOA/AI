window.APP_CONFIG = {
  API_BASE: "http://127.0.0.1:8081/api/v1",
  BACKEND_MODES: {
    local: {
      label: "Local",
      apiBase: "http://127.0.0.1:8081/api/v1"
    },
    nifi: {
      label: "NiFi",
      apiBase: "http://127.0.0.1:8081/api/v1"
    }
  },
  DEFAULT_BACKEND_MODE: "local",
  BACKEND_MODE_API: "api/v1/internal/backend-mode",
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
