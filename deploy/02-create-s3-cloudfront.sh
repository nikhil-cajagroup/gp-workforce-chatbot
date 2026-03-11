#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Step 2: Create S3 Bucket + CloudFront for Widget Assets
# ═══════════════════════════════════════════════════════════════
# This hosts the chatbot widget JS/CSS files that WordPress loads.
#
# Run: bash deploy/02-create-s3-cloudfront.sh
# ═══════════════════════════════════════════════════════════════

set -e
BUCKET_NAME="gp-chatbot-widget-assets-$(aws sts get-caller-identity --query Account --output text)"
REGION="eu-west-2"

echo "📦 Creating S3 bucket: $BUCKET_NAME"

# Create bucket
aws s3api create-bucket \
  --bucket "$BUCKET_NAME" \
  --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION" 2>/dev/null \
  && echo "  ✅ Bucket created" || echo "  ⏭️  Bucket already exists"

# Block all public access (CloudFront will serve it)
aws s3api put-public-access-block \
  --bucket "$BUCKET_NAME" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
  && echo "  ✅ Public access blocked"

# Enable CORS on S3 (for font/asset loading)
cat > /tmp/s3-cors.json << EOF
{
  "CORSRules": [
    {
      "AllowedHeaders": ["*"],
      "AllowedMethods": ["GET"],
      "AllowedOrigins": ["https://insightsqi.cajagroup.com"],
      "MaxAgeSeconds": 3600
    }
  ]
}
EOF

aws s3api put-bucket-cors \
  --bucket "$BUCKET_NAME" \
  --cors-configuration file:///tmp/s3-cors.json \
  && echo "  ✅ CORS configured"

echo ""
echo "📡 Creating CloudFront distribution..."
echo "   (This can take 5-10 minutes to deploy globally)"

# Create Origin Access Control for CloudFront → S3
OAC_ID=$(aws cloudfront create-origin-access-control \
  --origin-access-control-config \
    Name="gp-chatbot-oac",Description="OAC for chatbot widget",SigningProtocol=sigv4,SigningBehavior=always,OriginAccessControlOriginType=s3 \
  --query 'OriginAccessControl.Id' --output text 2>/dev/null) \
  && echo "  ✅ OAC created: $OAC_ID" || { OAC_ID=""; echo "  ⏭️  OAC may already exist"; }

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ S3 Bucket: $BUCKET_NAME"
echo ""
echo "⚠️  NEXT STEPS (do these in the AWS Console):"
echo ""
echo "1. Create CloudFront distribution in AWS Console:"
echo "   → Origin domain: $BUCKET_NAME.s3.$REGION.amazonaws.com"
echo "   → Origin access: Origin access control (OAC)"
echo "   → Viewer protocol: Redirect HTTP to HTTPS"
echo "   → Cache policy: CachingOptimized"
echo "   → Price class: Use only Europe and North America"
echo ""
echo "2. After creating, copy the S3 bucket policy CloudFront gives you"
echo "   and apply it to the bucket."
echo ""
echo "3. Build and upload widget:"
echo "   cd gp-chat-ui"
echo "   VITE_API_BASE=https://YOUR-EB-URL.eu-west-2.elasticbeanstalk.com \\"
echo "   VITE_API_KEY=YOUR_API_KEY \\"
echo "   npm run build:widget"
echo ""
echo "   aws s3 sync dist-widget/ s3://$BUCKET_NAME/ --delete"
echo ""
echo "4. Your widget URL will be:"
echo "   https://DISTRIBUTION_ID.cloudfront.net/gp-chatbot-widget.js"
echo "   https://DISTRIBUTION_ID.cloudfront.net/gp-chatbot-widget.css"
echo "═══════════════════════════════════════════════════════════"
