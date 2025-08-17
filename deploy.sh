#!/bin/bash
# deploy.sh

# Build React app
cd frontend
npm run build

# Upload to S3
aws s3 sync build/ s3://your-website-bucket --delete



echo "Frontend deployed successfully!"