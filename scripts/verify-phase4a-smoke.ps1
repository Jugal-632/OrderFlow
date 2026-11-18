$ErrorActionPreference = 'Stop'
$evidenceDirectory = Join-Path $PSScriptRoot '../docs/evidence'
New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
if (-not $env:BOOTSTRAP_CUSTOMER_PASSWORD) { throw 'Set BOOTSTRAP_CUSTOMER_PASSWORD before this historical smoke check' }
$loginBody = @{email=$env:BOOTSTRAP_CUSTOMER_EMAIL; password=$env:BOOTSTRAP_CUSTOMER_PASSWORD} | ConvertTo-Json
$correlation = [guid]::NewGuid().ToString()
$login = Invoke-WebRequest -Method Post -Uri http://localhost:8081/auth/login -ContentType application/json -Body $loginBody
$identity = $login.Content | ConvertFrom-Json
$token = if ($identity.token) { $identity.token } else { $identity.accessToken }
if ($login.StatusCode -ne 200 -or -not $token) { throw 'Login failed' }
$response = Invoke-WebRequest -Method Post -Uri http://localhost:8082/api/orders -ContentType application/json -Headers @{Authorization="Bearer $token"; 'X-Correlation-Id'=$correlation} -Body '{"customerId":"11111111-1111-1111-1111-111111111111","productId":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","quantity":2}'
$order = $response.Content | ConvertFrom-Json
if ($response.StatusCode -ne 201) { throw 'Create-order did not return 201' }
$orderId = [guid]::Parse($order.id).ToString()
$query = "SELECT payload::text FROM order_schema.outbox_events WHERE aggregate_id='$orderId'::uuid;"
$stored = docker exec orderflow-postgres psql -U orderflow -d orderflow_db -At -c $query
if ($LASTEXITCODE -ne 0) { throw 'Database read failed' }
$envelope = $stored | ConvertFrom-Json
if ($envelope.correlationId -ne $correlation) { throw 'Outbox correlation mismatch' }
$records = docker exec orderflow-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order.placed --from-beginning --timeout-ms 10000 --property print.headers=true 2>&1
$matched = @($records | Where-Object { $_.ToString().Contains($envelope.eventId) -and $_.ToString().Contains($correlation) })
if ($matched.Count -eq 0) { throw 'Matching Kafka event was not observed within timeout' }
if (-not ($matched | Where-Object { $_.ToString().Contains("X-Correlation-Id:$correlation") })) { throw 'Kafka correlation header mismatch' }
$result = [ordered]@{
    verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
    loginStatus = $login.StatusCode
    createOrderStatus = $response.StatusCode
    orderId = $orderId
    eventId = $envelope.eventId
    correlationId = $correlation
    httpToOutboxToKafka = 'PASS'
    kafkaRecords = @($matched | ForEach-Object { $_.ToString() })
}
$result | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 (Join-Path $evidenceDirectory 'phase4a-smoke.json')
$result | ConvertTo-Json -Depth 5
