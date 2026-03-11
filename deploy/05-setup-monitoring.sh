#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Step 5: Set Up Monitoring & Billing Alerts
# ═══════════════════════════════════════════════════════════════
#
# Run: bash deploy/05-setup-monitoring.sh
# ═══════════════════════════════════════════════════════════════

set -e
REGION="eu-west-2"
ALERT_EMAIL="${1:-}"

if [ -z "$ALERT_EMAIL" ]; then
    echo "Usage: bash deploy/05-setup-monitoring.sh your@email.com"
    exit 1
fi

echo "📊 Setting up monitoring for GP Chatbot"
echo "   Alert email: $ALERT_EMAIL"
echo ""

# ── SNS Topic for alerts ──────────────────────────────────────
TOPIC_ARN=$(aws sns create-topic \
  --name "gp-chatbot-alerts" \
  --region "$REGION" \
  --query 'TopicArn' --output text)
echo "  ✅ SNS topic: $TOPIC_ARN"

aws sns subscribe \
  --topic-arn "$TOPIC_ARN" \
  --protocol email \
  --notification-endpoint "$ALERT_EMAIL" \
  --region "$REGION" > /dev/null
echo "  ✅ Email subscription created — CHECK YOUR EMAIL to confirm!"

# ── Billing Alert: £40/month ──────────────────────────────────
echo ""
echo "💰 Creating billing alerts..."

aws cloudwatch put-metric-alarm \
  --alarm-name "gp-chatbot-billing-40" \
  --alarm-description "Monthly AWS bill exceeds £40" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --threshold 40 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions "$TOPIC_ARN" \
  --dimensions Name=Currency,Value=GBP \
  --region us-east-1 \
  && echo "  ✅ £40 billing alert created" || echo "  ⚠️  Billing alerts require us-east-1 and billing access"

aws cloudwatch put-metric-alarm \
  --alarm-name "gp-chatbot-billing-60" \
  --alarm-description "Monthly AWS bill exceeds £60 — investigate immediately" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --threshold 60 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions "$TOPIC_ARN" \
  --dimensions Name=Currency,Value=GBP \
  --region us-east-1 \
  && echo "  ✅ £60 billing alert created" || echo "  ⚠️  Billing alerts require us-east-1 and billing access"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "✅ Monitoring setup complete!"
echo ""
echo "Alerts configured:"
echo "  📧 Email: $ALERT_EMAIL (confirm the subscription email!)"
echo "  💰 £40/month billing warning"
echo "  💰 £60/month billing critical"
echo ""
echo "EB health monitoring is automatic — check in EB Console:"
echo "  → Enhanced health enabled by default"
echo "  → View in: EB Console → Environment → Health"
echo "═══════════════════════════════════════════════════════════"
