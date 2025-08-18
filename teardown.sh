#!/bin/bash
set -e

echo "🚨 Card Arbitrage Infrastructure Teardown"
echo "This will DESTROY all AWS resources and DATA will be LOST!"
echo "═══════════════════════════════════════════════════════════"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ Error: AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

echo "📋 Current AWS Account:"
aws sts get-caller-identity --query '[Account,Arn]' --output table

echo ""
echo "⚠️  WARNING: This will delete the following resources:"
echo "   • DynamoDB Tables (card-listings, arbitrage-opportunities)"
echo "   • Lambda Functions (all 5 functions)"
echo "   • S3 Website Bucket and ALL contents"
echo "   • API Gateway"
echo "   • Step Functions State Machine"
echo "   • CloudWatch Logs"
echo "   • IAM Roles and Policies"
echo "   • Secrets Manager entries"
echo "   • All scheduled events"
echo ""

# Confirmation prompt
read -p "❓ Are you sure you want to proceed? Type 'DELETE' to confirm: " confirmation

if [ "$confirmation" != "DELETE" ]; then
    echo "❌ Teardown cancelled. No resources were deleted."
    exit 0
fi

echo ""
echo "🔍 Checking if CardArbitrageStack exists..."

# Check if stack exists
if ! aws cloudformation describe-stacks --stack-name CardArbitrageStack > /dev/null 2>&1; then
    echo "ℹ️  CardArbitrageStack not found. Nothing to teardown."
    exit 0
fi

echo "✅ Stack found. Beginning teardown..."

# Get stack outputs before deletion for cleanup
echo "📤 Retrieving stack outputs..."
cd infrastructure

# Try to get outputs if available
if aws cloudformation describe-stacks --stack-name CardArbitrageStack --query 'Stacks[0].Outputs' > /dev/null 2>&1; then
    echo "📝 Stack outputs retrieved."
else
    echo "⚠️  Could not retrieve outputs, proceeding with stack deletion anyway."
fi

echo ""
echo "🗑️  Destroying CDK stack..."
cdk destroy --force

if [ $? -eq 0 ]; then
    echo "✅ CDK stack destroyed successfully!"
else
    echo "❌ CDK destroy failed. You may need to manually clean up resources."
    echo "💡 Try: aws cloudformation delete-stack --stack-name CardArbitrageStack"
    exit 1
fi

echo ""
echo "🧹 Performing additional cleanup..."

# Clean up any remaining resources that might not be caught by CDK
echo "🔍 Checking for orphaned resources..."

# Check for any remaining S3 buckets with our prefix
echo "   Checking S3 buckets..."
orphaned_buckets=$(aws s3 ls | grep "cardarbitragestack" | awk '{print $3}' || true)
if [ -n "$orphaned_buckets" ]; then
    echo "   ⚠️  Found orphaned S3 buckets: $orphaned_buckets"
    echo "   💡 You may want to manually delete these if they're not needed"
fi

# Check for any remaining Lambda functions
echo "   Checking Lambda functions..."
orphaned_lambdas=$(aws lambda list-functions --query 'Functions[?contains(FunctionName, `CardArbitrage`) || contains(FunctionName, `EbayScraper`) || contains(FunctionName, `ArbitrageDetector`)].FunctionName' --output text || true)
if [ -n "$orphaned_lambdas" ]; then
    echo "   ⚠️  Found orphaned Lambda functions: $orphaned_lambdas"
fi

# Check for any remaining DynamoDB tables
echo "   Checking DynamoDB tables..."
orphaned_tables=$(aws dynamodb list-tables --query 'TableNames[?contains(@, `card-listings`) || contains(@, `arbitrage-opportunities`)]' --output text || true)
if [ -n "$orphaned_tables" ]; then
    echo "   ⚠️  Found orphaned DynamoDB tables: $orphaned_tables"
fi

cd ..

echo ""
echo "🎉 Teardown completed!"
echo "═══════════════════════════════════════════"
echo "✅ All Card Arbitrage infrastructure has been destroyed"
echo "💰 AWS costs should now be minimized"
echo ""
echo "📝 Note: If you see any orphaned resources above, you may want to"
echo "   manually delete them from the AWS Console to ensure no ongoing costs."
echo ""
echo "🔄 To redeploy later, simply run: ./deploy.sh (or ./deploy.ps1 on Windows)"
