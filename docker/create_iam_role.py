"""Create/verify IAM role for Redshift/Athena access to project data."""

from __future__ import annotations

import json
import os

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

ROLE_NAME = os.getenv("REDSHIFT_IAM_ROLE_NAME", "CoffeeChainDataRole")
POLICIES = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
]
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": ["redshift.amazonaws.com", "athena.amazonaws.com"]},
            "Action": "sts:AssumeRole",
        }
    ],
}


def ensure_role(iam) -> str:
    try:
        role = iam.get_role(RoleName=ROLE_NAME)["Role"]
        print(f"✓ Role exists: {role['Arn']}")
        return role["Arn"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "NoSuchEntity":
            raise

    print(f"Creating role: {ROLE_NAME}")
    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        Description="Role for Coffee Chain Analytics (S3 + Athena access)",
    )["Role"]
    print(f"✓ Role created: {role['Arn']}")
    return role["Arn"]


def attach_policies(iam) -> None:
    attached = iam.list_attached_role_policies(RoleName=ROLE_NAME).get("AttachedPolicies", [])
    attached_arns = {p["PolicyArn"] for p in attached}
    for policy_arn in POLICIES:
        if policy_arn in attached_arns:
            print(f"  ✓ Already attached: {policy_arn}")
            continue
        iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy_arn)
        print(f"  ✓ Attached: {policy_arn}")


def main() -> int:
    print("=" * 50)
    print("IAM ROLE SETUP")
    print("=" * 50)
    print(f"Role name: {ROLE_NAME}")

    try:
        iam = boto3.client("iam")
        arn = ensure_role(iam)
        attach_policies(iam)
        print(f"\nREDSHIFT_IAM_ROLE={arn}")
        print("\n✓ IAM role setup complete")
        return 0
    except (NoCredentialsError, PartialCredentialsError):
        print("✗ Missing or incomplete AWS credentials")
        return 1
    except ClientError as e:
        err = e.response.get("Error", {})
        print(f"✗ {err.get('Code')} — {err.get('Message')}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
