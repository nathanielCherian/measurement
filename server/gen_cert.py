"""Generate a short-lived ECDSA P-256 certificate usable with WebTransport's
`serverCertificateHashes` (Chrome requires ECDSA and validity <= 14 days).

Writes certs/cert.pem, certs/key.pem and ../web/cert-hash.json.
"""

import argparse
import base64
import datetime
import hashlib
import ipaddress
import json
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10, help="validity (max 14)")
    parser.add_argument(
        "--san",
        action="append",
        default=[],
        help="extra DNS name or IP for subjectAltName (repeatable)",
    )
    args = parser.parse_args()
    assert 1 <= args.days <= 14, "WebTransport cert hashes require validity <= 14 days"

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "browser-cc-test")])

    alt_names = [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    for san in args.san:
        try:
            alt_names.append(x509.IPAddress(ipaddress.ip_address(san)))
        except ValueError:
            alt_names.append(x509.DNSName(san))

    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=args.days))
        .add_extension(x509.SubjectAlternativeName(alt_names), critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_dir = os.path.join(HERE, "certs")
    os.makedirs(cert_dir, exist_ok=True)
    with open(os.path.join(cert_dir, "cert.pem"), "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(os.path.join(cert_dir, "key.pem"), "wb") as f:
        f.write(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )

    digest = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).digest()
    hash_path = os.path.join(HERE, "..", "web", "cert-hash.json")
    with open(hash_path, "w") as f:
        json.dump(
            {
                "sha256_b64": base64.b64encode(digest).decode(),
                "not_after": cert.not_valid_after_utc.isoformat()
                if hasattr(cert, "not_valid_after_utc")
                else cert.not_valid_after.isoformat(),
            },
            f,
            indent=2,
        )
    print(f"wrote certs/ and web/cert-hash.json (sha256 {digest.hex()})")


if __name__ == "__main__":
    main()
