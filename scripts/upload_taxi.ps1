param(
    [Parameter(Mandatory = $true)]
    [string]$FilePath,

    [Parameter(Mandatory = $true)]
    [string]$Profile,

    [string]$Bucket
)

$ErrorActionPreference = "Stop"

if ($Bucket) {
    $bucket = $Bucket
} else {
    $bucket = terraform -chdir=terraform output -raw raw_bucket_name
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to resolve the raw S3 bucket. Pass -Bucket or initialize Terraform output."
    }
}
if (-not $bucket) { throw "The raw S3 bucket name is empty." }
$resolvedPath = (Resolve-Path -LiteralPath $FilePath).Path
$fileName = Split-Path $resolvedPath -Leaf

if ($fileName -notmatch '^yellow_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet$') {
    throw "Invalid filename: $fileName"
}

$year = $Matches[1]
$month = $Matches[2]
$key = "yellow/year=$year/month=$month/$fileName"

$hash = (Get-FileHash -LiteralPath $resolvedPath -Algorithm SHA256).Hash.ToLowerInvariant()

Write-Host "File: $fileName"
Write-Host "SHA-256: $hash"
Write-Host "Destination: s3://$bucket/$key"

# Prevent accidental reuploads, which trigger new Glue executions.
$existing = aws s3api list-objects-v2 `
    --bucket $bucket `
    --prefix $key `
    --query "Contents[?Key=='$key'].Key" `
    --output text `
    --profile $Profile

if ($LASTEXITCODE -ne 0) {
    throw "Unable to check existing S3 objects."
}

if ($existing) {
    throw "Object already exists. Upload cancelled to avoid duplicate processing."
}

aws s3 cp `
    $resolvedPath `
    "s3://$bucket/$key" `
    --metadata "sha256=$hash" `
    --profile $Profile `
    --no-progress

if ($LASTEXITCODE -ne 0) {
    throw "S3 upload failed."
}

$metadata = aws s3api head-object `
    --bucket $bucket `
    --key $key `
    --query "Metadata.sha256" `
    --output text `
    --profile $Profile

if ($LASTEXITCODE -ne 0 -or $metadata -ne $hash) {
    throw "SHA-256 metadata verification failed."
}

Write-Host "Upload successful. SHA-256 metadata verified."
Write-Host "Automatic ingestion should now start."
