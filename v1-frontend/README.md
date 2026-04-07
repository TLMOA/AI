# V1 Frontend (Simplified)

## Run

```bash
cd v1-frontend
python -m http.server 5174
```

Open: http://127.0.0.1:5174

## Config

Edit `config.js`:

- `API_BASE`: backend base url (`http://127.0.0.1:8081/api/v1`)
- `USE_MOCK_API`: true to use frontend mock adapter

## Current pages

- Create Job
- Job List
- Job Detail
- Output download link
- File Center (list + preview + download)
- Tag Center (manual tag + auto tag)

## Day3 updates

- Job type selector now includes `IMPORT_EXTERNAL` and `TAG_MANUAL` for full regression.
- Job detail now renders NiFi error hints by error code:
	- `NIFI_AUTH_ERROR`
	- `NIFI_NETWORK_ERROR`
	- `NIFI_FLOW_NOT_FOUND`
	- `NIFI_FLOW_UNMAPPED`
	- `NIFI_EXEC_ERROR`

## Day2 quick flow

1. Create a job and wait until status becomes `SUCCEEDED`.
2. Go to File Center and click `加载文件`.
3. Preview the generated file and test download.
4. Use Tag Center to submit manual tag or trigger auto tag.
