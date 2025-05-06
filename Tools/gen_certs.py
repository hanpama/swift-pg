import os
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PrivateFormat,
    NoEncryption,
)
from datetime import datetime, timedelta


def main():
    chains = [
        {
            "root": "root_ca_1",
            "children": [
                "postgres17.goodcn",
                "postgres16.goodcn",
                "postgres15.goodcn",
                "user_cert",
            ],
        },
        {
            "root": "root_ca_2",
            "children": [
                "user_cert",
            ],
        },
    ]
    for chain in chains:
        root_key, root_cert = _create_root_ca(chain["root"])
        root_key_bytes = root_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=NoEncryption(),
        )
        root_cert_bytes = root_cert.public_bytes(Encoding.PEM)
        with open(f"{chain['root']}.key.pem", "wb") as f:
            f.write(root_key_bytes)
        with open(f"{chain['root']}.crt.pem", "wb") as f:
            f.write(root_cert_bytes)

        for child in chain["children"]:
            child_key, child_csr = _create_key_and_csr(child)
            child_cert = _sign_cert(child_csr, root_cert, root_key)
            child_key_bytes = child_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=NoEncryption(),
            )
            child_cert_bytes = child_cert.public_bytes(Encoding.PEM)
            with open(f"{chain['root']}_{child}.key.pem", "wb") as f:
                f.write(child_key_bytes)
            with open(f"{chain['root']}_{child}.crt.pem", "wb") as f:
                f.write(child_cert_bytes)

    os.system("chmod 600 /certs/*.key.pem")
    os.system("chmod 600 /certs/*.crt.pem")
    os.system("chown 999:999 /certs/*.key")


def _create_key_and_csr(common_name):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(
            x509.Name(
                [
                    x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
                    x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "My Company"),
                    x509.NameAttribute(NameOID.COMMON_NAME, common_name),
                ]
            )
        )
        .sign(key, hashes.SHA256())
    )
    return key, csr


def _sign_cert(csr, issuer_cert, issuer_key, days=500):
    subject = csr.subject
    issuer = issuer_cert.subject
    cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer)
    cert = cert.public_key(csr.public_key()).serial_number(x509.random_serial_number())
    cert = cert.not_valid_before(datetime.now())
    cert = cert.not_valid_after(datetime.now() + timedelta(days=days))
    cert = cert.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )
    return cert.sign(issuer_key, hashes.SHA256())


def _create_root_ca(common_name):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "My Company"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )
    cert = x509.CertificateBuilder().subject_name(subject).issuer_name(issuer)
    cert = cert.public_key(key.public_key()).serial_number(x509.random_serial_number())
    cert = cert.not_valid_before(datetime.now())
    cert = cert.not_valid_after(datetime.now() + timedelta(days=1024))
    cert = cert.add_extension(
        x509.BasicConstraints(ca=True, path_length=None), critical=True
    )
    return key, cert.sign(key, hashes.SHA256())


if __name__ == "__main__":
    main()
