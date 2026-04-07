# Remaining Manual Steps

## 1. Subscribe Email to SNS Alerts

Run these commands with your notification email address:

```bash
# CloudWatch alarms (eu-west-2)
aws sns subscribe \
  --topic-arn arn:aws:sns:eu-west-2:736116164248:gp-chatbot-alerts \
  --protocol email \
  --notification-endpoint YOUR_EMAIL@example.com \
  --region eu-west-2

# Billing alarms (us-east-1)
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:736116164248:gp-chatbot-billing-alerts \
  --protocol email \
  --notification-endpoint YOUR_EMAIL@example.com \
  --region us-east-1
```

Then confirm both subscription emails that arrive in your inbox.

## 2. Add WordPress Embed Code

Add this as a **Custom HTML block** on your WordPress page:

```html
<link rel="stylesheet" href="https://d2osieaua9f58r.cloudfront.net/gp-chatbot-widget.css" />
<div id="gp-chatbot-root"></div>
<script src="https://d2osieaua9f58r.cloudfront.net/gp-chatbot-widget.js"></script>
```

## 3. Test End-to-End

1. Add the embed code to WordPress
2. Visit https://insightsqi.cajagroup.com/primary-care-gp-practice-workforce/
3. Verify the chatbot loads and responds to queries
4. Test with 2 team members as per Phase 5
5. Monitor for 1 week, then remove debug panel from UI
