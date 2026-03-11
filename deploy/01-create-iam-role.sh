#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Step 1: Create IAM Role for Elastic Beanstalk
# ═══════════════════════════════════════════════════════════════
# This role lets the EB instance access Bedrock, Athena, and S3
# without needing any access keys.
#
# Run: bash deploy/01-create-iam-role.sh
# ═══════════════════════════════════════════════════════════════

set -e
ROLE_NAME="gp-chatbot-backend-role"
INSTANCE_PROFILE_NAME="gp-chatbot-backend-profile"
REGION="eu-west-2"

echo "📋 Creating IAM Role: $ROLE_NAME"

# Trust policy — allows EC2 instances to assume this role
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the role
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/trust-policy.json \
  --description "GP Workforce Chatbot backend - Bedrock, Athena, S3 access" \
  --region "$REGION" 2>/dev/null && echo "  ✅ Role created" || echo "  ⏭️  Role already exists"

# Custom policy — minimum permissions needed
echo "📋 Attaching permissions policy..."

cat > /tmp/chatbot-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BedrockInvoke",
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AthenaQuery",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueReadSchema",
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3AthenaResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::test-athena-results-fingertips",
        "arn:aws:s3:::test-athena-results-fingertips/*"
      ]
    },
    {
      "Sid": "S3DataRead",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::*fingertips*",
        "arn:aws:s3:::*fingertips*/*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "gp-chatbot-permissions" \
  --policy-document file:///tmp/chatbot-policy.json \
  && echo "  ✅ Permissions attached"

# Also attach the EB managed policies (needed for EB health reporting)
aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/AWSElasticBeanstalkWebTier" 2>/dev/null \
  && echo "  ✅ EB Web Tier policy attached" || echo "  ⏭️  Already attached"

# Create instance profile (connects the role to EC2 instances)
aws iam create-instance-profile \
  --instance-profile-name "$INSTANCE_PROFILE_NAME" 2>/dev/null \
  && echo "  ✅ Instance profile created" || echo "  ⏭️  Already exists"

aws iam add-role-to-instance-profile \
  --instance-profile-name "$INSTANCE_PROFILE_NAME" \
  --role-name "$ROLE_NAME" 2>/dev/null \
  && echo "  ✅ Role added to instance profile" || echo "  ⏭️  Already added"

echo ""
echo "✅ Done! IAM Role: $ROLE_NAME"
echo "   Instance Profile: $INSTANCE_PROFILE_NAME"
echo ""
echo "Next: Run deploy/02-create-s3-cloudfront.sh"
