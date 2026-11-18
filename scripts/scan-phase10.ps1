$ErrorActionPreference = 'Stop'
$scanner = 'aquasec/trivy@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c'
$reports = (Resolve-Path 'docs/evidence/phase10').Path
$tag = if ($env:IMAGE_TAG) { $env:IMAGE_TAG } else { 'local' }
$failed = $false
foreach ($service in @('auth-service','order-service','inventory-service','notification-service','api-gateway','frontend')) {
    $image = "orderflow/${service}:$tag"
    docker image inspect $image | Set-Content -Encoding utf8 "$reports/$service-image.json"
    docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v orderflow-trivy-cache:/root/.cache/trivy -v "${reports}:/reports" $scanner image --quiet --scanners vuln,secret --severity HIGH,CRITICAL --exit-code 1 --format json --output "/reports/$service-scan.json" $image
    if ($LASTEXITCODE -ne 0) { $failed = $true }
    docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v orderflow-trivy-cache:/root/.cache/trivy -v "${reports}:/reports" $scanner image --quiet --format cyclonedx --output "/reports/$service-sbom.json" $image
    if ($LASTEXITCODE -ne 0) { $failed = $true }
}
if ($failed) { exit 1 }
