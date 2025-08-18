#!/bin/bash
set -e

echo "Starting Card Arbitrage deployment..."

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "Error: AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed. Please install jq first."
    echo "On Windows: choco install jq"
    echo "On MacOS: brew install jq" 
    echo "On Ubuntu: sudo apt-get install jq"
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

# Get outputs - using the correct output names from our stack
API_GATEWAY_ENDPOINT=$(jq -r '.CardArbitrageStack.ApiEndpoint' outputs.json)
WEBSITE_URL=$(jq -r '.CardArbitrageStack.WebsiteUrl' outputs.json)
BUCKET_NAME=$(jq -r '.CardArbitrageStack.WebsiteBucketOutput' outputs.json)

echo "API Endpoint: $API_GATEWAY_ENDPOINT"
echo "Website URL: $WEBSITE_URL"
echo "Bucket Name: $BUCKET_NAME"

# Update frontend with API endpoint
cd ../frontend

# Create .env file for frontend with the API endpoint
echo "REACT_APP_API_URL=$API_GATEWAY_ENDPOINT" > .env

echo "Updated frontend .env file with API endpoint"

# Build and deploy frontend
echo "Installing frontend dependencies..."
npm install

echo "Building frontend..."
npm run build

echo "Deploying frontend to S3..."
aws s3 sync build/ "s3://$BUCKET_NAME" --delete

echo "Deployment completed successfully!"
echo "Website URL: $WEBSITE_URL"
echo "API Endpoint: $API_GATEWAY_ENDPOINT"
echo ""
echo "Next steps:"
echo "1. Configure eBay API credentials in AWS Secrets Manager:"
echo "   aws secretsmanager update-secret --secret-id card-arbitrage/ebay-credentials --secret-string '{\"client_id\":\"your-ebay-client-id\",\"client_secret\":\"your-ebay-client-secret\",\"sandbox\":\"true\"}'"
echo "2. Test the API endpoint: curl $API_GATEWAY_ENDPOINT/health"
echo "3. Visit your website: $WEBSITE_URL"