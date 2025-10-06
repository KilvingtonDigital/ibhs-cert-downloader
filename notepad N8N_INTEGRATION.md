\# n8n Integration Guide for IBHS Certificate Downloader



\## Overview

This Actor is optimized to work seamlessly with n8n workflows for automated certificate processing.



\## Key Features for n8n



\### ✅ Structured Output

Every run returns clean JSON with:

\- `success`: boolean (true/false) for easy filtering

\- `status`: detailed status code

\- `timestamp`: ISO 8601 timestamp

\- All certificate data (FH number, dates, addresses)

\- Download URLs (Google Drive or Apify storage)



\### ✅ Batch Processing

\- Process single addresses or arrays

\- Set `maxAddressesPerRun` to control batch size

\- Automatic rate limiting with `politeDelayMs`



\### ✅ Error Handling

\- Every address gets a result (success or error)

\- Detailed error messages in `error` field

\- `success: false` for failed addresses



---



\## Setup in Apify



\### 1. Deploy the Actor



```bash

\# In your project folder

apify push

```



\### 2. Set Secrets in Apify Console



Go to: https://console.apify.com/actors



Click your actor → \*\*Settings\*\* → \*\*Environment Variables\*\*



Add:

\- `IBHS\_USERNAME` = your-email@example.com

\- `IBHS\_PASSWORD` = your-password

\- `GOOGLE\_DRIVE\_FOLDER\_ID` = your-folder-id (optional)

\- `GOOGLE\_SERVICE\_ACCOUNT\_EMAIL` = service-account@project.iam.gserviceaccount.com (optional)

\- `GOOGLE\_SERVICE\_ACCOUNT\_KEY` = -----BEGIN PRIVATE KEY----- (optional)



---



\## n8n Workflow Setup



\### Node 1: Trigger (Webhook, Schedule, or Manual)



\*\*Example: Webhook\*\*

```json

{

&nbsp; "address": "513 MALAGA DRIVE"

}

```



\### Node 2: Apify Node



\*\*Settings:\*\*

\- \*\*Operation\*\*: Call Actor

\- \*\*Actor ID\*\*: `your-username/ibhs-cert-downloader`

\- \*\*Input JSON\*\*:



```json

{

&nbsp; "address": "{{ $json.address }}",

&nbsp; "returnStructuredData": true,

&nbsp; "debug": false

}

```



\*\*Or for batch processing:\*\*

```json

{

&nbsp; "addresses": {{ $json.addresses }},

&nbsp; "maxAddressesPerRun": 10

}

```



\### Node 3: Wait for Results



\- \*\*Operation\*\*: Wait for run to finish

\- \*\*Timeout\*\*: 300 seconds (adjust based on batch size)



\### Node 4: Get Dataset Items



\- \*\*Operation\*\*: Get dataset items

\- \*\*Dataset ID\*\*: `{{ $json.defaultDatasetId }}`



---



\## Example n8n Workflows



\### Workflow 1: Single Address Processing



```

Webhook → Apify (Call Actor) → Wait → Get Dataset → Filter Success → Process Results

```



\*\*Webhook Input:\*\*

```json

{

&nbsp; "address": "520 Novatan Rd S, Mobile, AL 36608"

}

```



\*\*Apify Input:\*\*

```json

{

&nbsp; "address": "{{ $json.address }}"

}

```



\*\*Filter Node (Success Only):\*\*

```javascript

return items.filter(item => item.json.success === true);

```



\### Workflow 2: Batch Processing from Google Sheets



```

Google Sheets (Read) → Split In Batches → Apify → Wait → Get Dataset → Update Sheet

```



\*\*Split In Batches:\*\*

\- Batch size: 10 addresses



\*\*Apify Input:\*\*

```json

{

&nbsp; "addresses": {{ $json.addresses }},

&nbsp; "maxAddressesPerRun": 10

}

```



\### Workflow 3: Download PDFs to Local Storage



```

Apify → Get Dataset → Filter Success → HTTP Request (Download PDF) → Write Binary File

```



\*\*HTTP Request Node:\*\*

\- \*\*URL\*\*: `{{ $json.googleDriveLink }}` (if using Google Drive)

\- \*\*OR\*\*: `{{ $json.apifyDownloadUrl }}` (if using Apify storage)

\- \*\*Response Format\*\*: Binary



---



\## Output Schema



\### Success Response:

```json

{

&nbsp; "address": "520 NOVATAN ROAD",

&nbsp; "searchAddress": "520 novatan road",

&nbsp; "status": "downloaded",

&nbsp; "success": true,

&nbsp; "fhNumber": "FH25016154",

&nbsp; "buildingAddress": "520 Novatan Rd S",

&nbsp; "approvedAt": "08/08/2025",

&nbsp; "expirationDate": "08/08/2030",

&nbsp; "certificateFile": "fh25016154-certificate.pdf",

&nbsp; "fileName": "FH25016154 - Expires 08-08-2030.pdf",

&nbsp; "fileSize": 245678,

&nbsp; "googleDriveFileId": "1ABC...XYZ",

&nbsp; "googleDriveLink": "https://drive.google.com/file/d/1ABC...XYZ/view",

&nbsp; "apifyDownloadUrl": "https://api.apify.com/v2/key-value-stores/.../records/...",

&nbsp; "timestamp": "2025-10-06T14:30:00.000Z",

&nbsp; "downloadedAt": "2025-10-06T14:30:00.000Z",

&nbsp; "processedAt": "2025-10-06T14:30:00.000Z"

}

```



\### Error Response:

```json

{

&nbsp; "address": "INVALID ADDRESS",

&nbsp; "searchAddress": "invalid address",

&nbsp; "status": "no\_certificate",

&nbsp; "success": false,

&nbsp; "error": "No download button found",

&nbsp; "fhNumber": null,

&nbsp; "buildingAddress": null,

&nbsp; "approvedAt": null,

&nbsp; "expirationDate": null,

&nbsp; "timestamp": "2025-10-06T14:30:00.000Z",

&nbsp; "processedAt": "2025-10-06T14:30:00.000Z"

}

```



---



\## Advanced n8n Patterns



\### Pattern 1: Conditional Routing



```javascript

// Route based on status

if (item.json.success) {

&nbsp; // Success path: Upload to database

&nbsp; return { json: item.json };

} else {

&nbsp; // Error path: Send notification

&nbsp; return { json: { error: item.json.error, address: item.json.address } };

}

```



\### Pattern 2: Retry Failed Addresses



```javascript

// Collect failed addresses

const failedAddresses = items

&nbsp; .filter(item => item.json.success === false)

&nbsp; .map(item => item.json.address);



return \[{ json: { addresses: failedAddresses } }];

```



\### Pattern 3: Data Enrichment



```javascript

// Combine with external data

return {

&nbsp; json: {

&nbsp;   ...item.json,

&nbsp;   customerId: customerLookup\[item.json.address],

&nbsp;   region: getRegion(item.json.buildingAddress),

&nbsp;   daysUntilExpiration: calculateDays(item.json.expirationDate)

&nbsp; }

};

```



---



\## Best Practices



\### 1. Rate Limiting

\- Don't process more than 50 addresses per run

\- Use `politeDelayMs: 1000` for production

\- Increase delays if you hit rate limits



\### 2. Error Handling

\- Always filter by `success: true/false`

\- Log failed addresses for manual review

\- Retry failed addresses with exponential backoff



\### 3. Cost Optimization

\- Use `maxAddressesPerRun` to limit compute units

\- Set reasonable timeouts in n8n

\- Cache results to avoid duplicate lookups



\### 4. Monitoring

\- Track `success` rate over time

\- Alert on high failure rates

\- Monitor processing time per address



---



\## Troubleshooting



\### Issue: Timeout in n8n

\*\*Solution\*\*: Increase timeout or reduce `maxAddressesPerRun`



\### Issue: No results returned

\*\*Solution\*\*: Check if `returnStructuredData: true` is set



\### Issue: PDFs not downloading

\*\*Solution\*\*: Verify Google Drive credentials or use `apifyDownloadUrl`



\### Issue: Wrong address selected

\*\*Solution\*\*: Review input format, ensure full address with street number



---



\## API Authentication



\### Option 1: Apify API Token (Recommended)

```javascript

// In n8n HTTP Request node

Headers: {

&nbsp; "Authorization": "Bearer YOUR\_APIFY\_TOKEN"

}

```



\### Option 2: Apify Integration Node

\- Use built-in Apify credential type

\- No manual token management needed



---



\## Cost Estimation



\*\*Compute Units per Address:\*\*

\- Average: 0.02 CU per address

\- With debug mode: 0.05 CU per address



\*\*Example Monthly Costs:\*\*

\- 1,000 addresses: ~20 CU = $1

\- 10,000 addresses: ~200 CU = $10



---



\## Support



For issues specific to:

\- \*\*Actor functionality\*\*: Check Apify logs

\- \*\*n8n integration\*\*: Check n8n execution logs

\- \*\*IBHS website changes\*\*: Update selectors in `main.js`

