# two_factor_manager.py
import pyotp
import qrcode
import base64
from io import BytesIO

class TwoFactorManager:
    def __init__(self):
        pass

    def generate(self, account_name, issuer_name="mgindb"):
        """
        Generate a new 2FA secret and QR code.

        :param account_name: The name of the account (e.g., user email)
        :param issuer_name: The name of the issuer (default is 'mgindb')
        :return: A dictionary with the secret and QR code URL
        """
        secret = pyotp.random_base32()
        totp = pyotp.TOTP(secret)
        uri = totp.provisioning_uri(name=account_name, issuer_name=issuer_name)
        
        # Generate QR code
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(uri)
        qr.make(fit=True)
        img = qr.make_image(fill='black', back_color='white')

        buffered = BytesIO()
        img.save(buffered, 'PNG')
        qr_code_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

        return {"secret": secret, "qr_code": f"data:image/png;base64,{qr_code_base64}"}

    def verify(self, secret, code):
        """
        Verify a 2FA code against a secret.

        :param secret: The 2FA secret
        :param code: The 2FA code to verify
        :return: True if the code is valid, False otherwise
        """
        totp = pyotp.TOTP(secret)
        return totp.verify(code)
