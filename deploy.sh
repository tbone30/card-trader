#!/bin/bash
set -e

echo "Starting Card Arbitrage deployment..."

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "Error: AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

# Check if CDK is bootstrapped
echo "Checking CDK bootstrap status..."
if ! aws cloudformation describe-stacks --stack-name CDKToolkit > /dev/null 2>&1; then
    echo "CDK not bootstrapped. Running cdk bootstrap..."
    cd infrastructure
    cdk bootstrap
    cd ..
fi

# Deploy infrastructure
cd infrastructure
echo "Installing CDK dependencies..."
pip install -r requirements.txt

echo "Deploying infrastructure..."
cdk deploy --require-approval never --outputs-file outputs.json

# Get outputs
API_ENDPOINT=$(jq -r '.CardArbitrageStack.ApiEndpoint' outputs.json)
WEBSITE_URL=$(jq -r '.CardArbitrageStack.WebsiteUrl' outputs.json)
BUCKET_NAME=$(jq -r '.CardArbitrageStack.WebsiteBucket' outputs.json)

echo "API Endpoint: $API_ENDPOINT"
echo "Website URL: $WEBSITE_URL"

# Update frontend with API endpoint
cd ../frontend
sed -i "s|https://your-api-gateway-url.amazonaws.com|$API_ENDPOINT|g" src/CardArbitrageDashboard.js

# Build and deploy frontend
echo "Building frontend..."
npm install
npm run build

echo "Deploying frontend..."
aws s3 sync build/ "s3://$BUCKET_NAME" --delete

echo "Deployment completed!"
echo "Website URL: $WEBSITE_URL"
echo "API Endpoint: $API_ENDPOINT"