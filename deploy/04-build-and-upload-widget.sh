#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Step 4: Build Widget & Upload to S3/CloudFront
# ═══════════════════════════════════════════════════════════════
# Builds the production widget and uploads to S3.
#
# Run: bash deploy/04-build-and-upload-widget.sh
# ═══════════════════════════════════════════════════════════════

set -e

# ── CONFIGURATION — Update these before running! ─────────────
API_BASE="${VITE_API_BASE:-https://YOUR-EB-URL.eu-west-2.elasticbeanstalk.com}"
API_KEY="${VITE_API_KEY:-YOUR_API_KEY_HERE}"
S3_BUCKET="${WIDGET_S3_BUCKET:-gp-chatbot-widget-assets-736116164248}"
CLOUDFRONT_ID="${CLOUDFRONT_DISTRIBUTION_ID:-}"
# ──────────────────────────────────────────────────────────────

echo "🔨 Building production widget..."
echo "   API_BASE: $API_BASE"
echo ""

cd gp-chat-ui

# Build the widget with production API URL baked in
VITE_API_BASE="$API_BASE" VITE_API_KEY="$API_KEY" npm run build:widget

echo ""
echo "📦 Build output:"
ls -lh dist-widget/

echo ""
echo "☁️  Uploading to S3: $S3_BUCKET"

aws s3 sync dist-widget/ "s3://$S3_BUCKET/" \
  --delete \
  --cache-control "public, max-age=3600" \
  && echo "  ✅ Uploaded to S3"

# Invalidate CloudFront cache (if distribution ID provided)
if [ -n "$CLOUDFRONT_ID" ]; then
    echo ""
    echo "🔄 Invalidating CloudFront cache..."
    aws cloudfront create-invalidation \
      --distribution-id "$CLOUDFRONT_ID" \
      --paths "/*" \
      --query 'Invalidation.Id' --output text \
      && echo "  ✅ Cache invalidated (takes 1-2 minutes to propagate)"
fi

cd ..

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ Widget deployed!"
echo ""
echo "Update your WordPress embed code to:"
echo "  <link rel=\"stylesheet\" href=\"https://CLOUDFRONT_DOMAIN/gp-chatbot-widget.css\" />"
echo "  <script src=\"https://CLOUDFRONT_DOMAIN/gp-chatbot-widget.js\" defer></script>"
echo "═══════════════════════════════════════════════════════════"
