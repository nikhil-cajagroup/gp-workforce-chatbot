#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Step 3: Deploy Backend to Elastic Beanstalk
# ═══════════════════════════════════════════════════════════════
# Packages the Docker app and deploys to EB.
#
# PREREQUISITES:
#   - Install EB CLI: pip install awsebcli
#   - Run 01-create-iam-role.sh first
#
# Run: bash deploy/03-deploy-elastic-beanstalk.sh
# ═══════════════════════════════════════════════════════════════

set -e
APP_NAME="gp-chatbot"
ENV_NAME="gp-chatbot-prod"
REGION="eu-west-2"
INSTANCE_PROFILE="gp-chatbot-backend-profile"

echo "🚀 Deploying GP Chatbot Backend to Elastic Beanstalk"
echo ""

# Check if EB CLI is installed
if ! command -v eb &> /dev/null; then
    echo "❌ EB CLI not found. Install it with:"
    echo "   pip install awsebcli"
    exit 1
fi

# Initialise EB (first time only)
if [ ! -d ".elasticbeanstalk" ]; then
    echo "📋 Initialising Elastic Beanstalk application..."
    eb init "$APP_NAME" \
      --platform "Docker" \
      --region "$REGION" \
      --keyname "" 2>/dev/null \
      && echo "  ✅ EB initialised"
fi

# Check if environment already exists
if eb status "$ENV_NAME" 2>/dev/null | grep -q "Status"; then
    echo "📋 Environment exists — deploying update..."
    eb deploy "$ENV_NAME"
else
    echo "📋 Creating new environment: $ENV_NAME"
    echo "   Instance type: t3.micro (free tier eligible)"
    echo "   This takes 5-10 minutes..."
    echo ""

    eb create "$ENV_NAME" \
      --single \
      --instance_type t3.micro \
      --instance_profile "$INSTANCE_PROFILE" \
      --region "$REGION" \
      --timeout 15
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ Deployment complete!"
echo ""

# Get the environment URL
EB_URL=$(eb status "$ENV_NAME" 2>/dev/null | grep "CNAME" | awk '{print $2}')
if [ -n "$EB_URL" ]; then
    echo "🌐 Backend URL: http://$EB_URL"
    echo ""
    echo "Test it:"
    echo "  curl http://$EB_URL/health"
fi

echo ""
echo "⚠️  NEXT STEPS:"
echo ""
echo "1. Set environment variables in EB Console or CLI:"
echo "   eb setenv \\"
echo "     AWS_REGION=eu-west-2 \\"
echo "     ATHENA_DATABASE=test-gp-workforce \\"
echo "     ATHENA_OUTPUT_S3=s3://test-athena-results-fingertips/ \\"
echo "     BEDROCK_CHAT_MODEL_ID=amazon.nova-pro-v1:0 \\"
echo "     CORS_ORIGINS=https://insightsqi.cajagroup.com \\"
echo "     API_KEY=YOUR_GENERATED_KEY \\"
echo "     LOG_LEVEL=INFO"
echo ""
echo "2. Enable HTTPS (in EB Console → Configuration → Load balancer)"
echo ""
echo "3. Then rebuild the widget with the EB URL:"
echo "   cd gp-chat-ui"
echo "   VITE_API_BASE=http://$EB_URL VITE_API_KEY=YOUR_KEY npm run build:widget"
echo "═══════════════════════════════════════════════════════════"
