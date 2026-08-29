import unittest
from echo.work_envelope import ExecutionReceipt, ReceiptChain, WorkEnvelope, sha256_hex, verify_receipt_chain

class ContractTests(unittest.TestCase):
    def envelope(self):
        return WorkEnvelope.create(work_id="w-1", idempotency_key="idem-1", producer="echo", source_repository="GlacierEQ/ECHO", source_revision="abc123", capability="inspect", authority_scope="casey-approved-read-only", exact_target="README.md", created_at="2026-08-25T14:33:00Z", payload={"ref": "main"})

    def test_round_trip_and_deterministic_digest(self):
        e = self.envelope(); self.assertEqual(WorkEnvelope.from_dict(e.as_dict()), e); self.assertEqual(e.envelope_sha256, WorkEnvelope.from_dict(e.as_dict()).envelope_sha256)

    def test_chain_binds_target_and_links_receipts(self):
        e = self.envelope(); chain = ReceiptChain(e)
        chain.append(status="succeeded", output={"files": ["README.md"]}, verified=True, verification_method="readback", created_at="2026-08-25T14:34:00Z")
        chain.append(status="succeeded", output={"checked": True}, verified=True, verification_method="deterministic-check", created_at="2026-08-25T14:35:00Z")
        self.assertTrue(chain.verify()); self.assertTrue(verify_receipt_chain(e, chain.receipts)); self.assertEqual(chain.receipts[1].previous_receipt_hash, chain.receipts[0].receipt_hash)

    def test_contract_rejects_write_mode_and_external_action(self):
        e = self.envelope()
        with self.assertRaises(ValueError): WorkEnvelope(**{**e.unsigned_dict(), "action_mode": "write"})
        with self.assertRaises(ValueError): ExecutionReceipt(e.work_id, e.envelope_sha256, "succeeded", True, "readback", sha256_hex({}), "2026-08-25T14:36:00Z", external_actions_performed=1)

    def test_tampering_breaks_verification(self):
        e = self.envelope(); chain = ReceiptChain(e); r = chain.append(status="succeeded", output={"ok": True}, verified=True, verification_method="readback", created_at="2026-08-25T14:37:00Z")
        self.assertTrue(chain.verify())
        raw = r.as_dict(); raw['details'] = {'ok': False}
        from echo.work_envelope import ExecutionReceipt
        with self.assertRaises(ValueError): ExecutionReceipt.from_dict(raw)

if __name__ == '__main__': unittest.main()
