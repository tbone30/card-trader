#!/bin/bash
# deploy.sh

# Build React app
cd frontend
npm run build

# Upload to S3
aws s3 sync build/ s3://your-website-bucket --delete

# Invalidate CloudFront cache
aws cloudfront create-invalidation --distribution-id YOUR_DISTRIBUTION_ID --paths "/*"

echo "Frontend deployed successfully!"