# -*- coding: utf8 -*-

from tests.lib.api_test_base import APITestBase
from tablestore.credentials import StaticCredentialsProvider
from tablestore.auth import *


class AuthTest(APITestBase):
    def setUp(self):
        self.test_ak_id = "test_id"
        self.test_ak_secret = "test_key"
        self.test_sts_token = "test_token"
        self.test_encoding = "utf-8"
        self.test_region = "test-region"
        self.test_sign_date = "20250410"
        self.signature_string = "test_signature_string"
        self.test_query = "test_query"
        self.headers = {"x-ots-test": "test"}

    def tearDown(self):
        pass  # no need to tearDown client

    def test_calculate_signature(self):
        actual_sha1_sign = b"C845e" + b"f7UjN" + b"GL0gE" + b"xNlQh" + b"p+3B" + b"/gY="
        sha1_sign = call_signature_method_sha1(self.test_ak_secret, self.signature_string, self.test_encoding)
        self.assertEqual(28, len(sha1_sign))
        self.assertEqual(actual_sha1_sign, sha1_sign)

        actual_sha256_sign = b"c+lCAaaQ" + b"VSCVlc0" + b"u0JBE" + b"PoIzy" + b"xplf4" + b"xEIBH" + b"8sdW" + b"UOjo="
        sha256_sign = call_signature_method_sha256(self.test_ak_secret, self.signature_string, self.test_encoding)
        self.assertEqual(44, len(sha256_sign))
        self.assertEqual(actual_sha256_sign, sha256_sign)

    def test_SignClass(self):
        cred = StaticCredentialsProvider(self.test_ak_id, self.test_ak_secret, self.test_sts_token)

        # test v2 sign
        v2_signer = SignV2(cred, self.test_encoding)
        self.assertEqual(cred, v2_signer.get_credentials_provider())
        self.assertEqual(None, v2_signer.signing_key)
        self.assertEqual(self.test_encoding, v2_signer.encoding)
        v2_signer.gen_signing_key()
        self.assertEqual(self.test_ak_secret, v2_signer.signing_key)
        test_headers = self.headers.copy()
        v2_signer.make_request_signature_and_add_headers(self.test_query, test_headers)
        v2_request_signature = test_headers[consts.OTS_HEADER_SIGNATURE]
        actual_v2_request_signature = b"QDhzL" + b"v7VES" + b"BJtYQY4" + b"Li0Ih" + b"SUOdg="
        self.assertEqual(28, len(v2_request_signature))
        self.assertEqual(actual_v2_request_signature, v2_request_signature)
        v2_response_signature = v2_signer.make_response_signature(self.test_query, self.headers)
        actual_v2_response_signature = b"UjJK" + b"/SWed0" + b"n9o6J" + b"YxvAp" + b"HGaQ" + b"ABo="
        self.assertEqual(actual_v2_response_signature, v2_response_signature)

        # test v4 sign
        with self.assertRaisesRegex(OTSClientError, "region is not str or is empty."):
            v4_signer = SignV4(cred, self.test_encoding)
        v4_signer = SignV4(cred, self.test_encoding, region=self.test_region, sign_date=self.test_sign_date)
        self.assertEqual(cred, v4_signer.get_credentials_provider())
        self.assertEqual(None, v4_signer.signing_key)
        self.assertEqual(self.test_encoding, v4_signer.encoding)
        v4_signer.gen_signing_key()
        actual_v4_signing_key = b"nToxlXr" + b"xgCm0L" + b"5J0nr/q" + b"q/GmtgN9" + b"GVBhiR" + b"LzdL" + b"aVUP0="
        self.assertEqual(actual_v4_signing_key, v4_signer.signing_key)
        test_headers = self.headers.copy()
        v4_signer.make_request_signature_and_add_headers(self.test_query, test_headers)
        self.assertEqual(self.test_region, test_headers[consts.OTS_HEADER_SIGN_REGION])
        self.assertEqual(v4_signer.sign_date, test_headers[consts.OTS_HEADER_SIGN_DATE])
        v4_request_signature = test_headers[consts.OTS_HEADER_SIGNATURE_V4]
        actual_v4_request_signature = b"yXnO" + b"pODWa" + b"U1EYAl" + b"LP3l25k" + b"sj010" + b"uGHS7" + b"uxIt5Q" + b"iwz4o="
        self.assertEqual(44, len(v4_request_signature))
        self.assertEqual(actual_v4_request_signature, v4_request_signature)
        v4_response_signature = v4_signer.make_response_signature(self.test_query, self.headers)
        actual_v4_response_signature = b"vIhaU" + b"Gwv/J" + b"Sg8ct" + b"LNyx" + b"bNeN" + b"v69A="
        self.assertEqual(actual_v4_response_signature, v4_response_signature)

        # v2 sign and v4 sign use the same response sign method(sha1)
        origin_v2_signing_key = v2_signer.signing_key
        v2_signer.signing_key = v4_signer.signing_key  # set signing key to same
        self.assertEqual(
            v2_signer.make_response_signature(self.test_query, self.headers),
            v4_signer.make_response_signature(self.test_query, self.headers)
        )
        v2_signer.signing_key = origin_v2_signing_key
