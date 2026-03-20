#!/usr/bin/env python3
"""Surat Kargo webservices — kargo takip hareket detayı SOAP çağrıları (CLI).

Metotlar (aynı parametreler: CariKodu, Sifre, WebSiparisKodu):
  1 — KargoTakipHareketDetayi
  2 — KargoTakipHareketDetayli
  3 — KargoTakipHareketDetayliV2

Örnek:
  python scripts/surat_kargo_takip_hareket_detayi.py --method 1 \\
    --cari-kodu CARI001 --sifre '***' --web-siparis-kodu SIP123
"""

from __future__ import annotations

import argparse
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

DEFAULT_URL = "https://webservices.suratkargo.com.tr/services.asmx"
TEMURI_NS = "http://tempuri.org/"
TEMURI = "{" + TEMURI_NS + "}"


@dataclass(frozen=True)
class Operation:
    key: str
    soap_method: str  # öğe adı + SOAPAction son parçası

    @property
    def soap_action(self) -> str:
        return f"http://tempuri.org/{self.soap_method}"

    @property
    def result_tag(self) -> str:
        return f"{self.soap_method}Result"


OPERATIONS: dict[str, Operation] = {
    "1": Operation("1", "KargoTakipHareketDetayi"),
    "2": Operation("2", "KargoTakipHareketDetayli"),
    "3": Operation("3", "KargoTakipHareketDetayliV2"),
}


def _xml_escape(s: str) -> str:
    return escape(s, {'"': "&quot;", "'": "&apos;"})


def _envelope_11(op: Operation, cari_kodu: str, sifre: str, web_siparis_kodu: str) -> str:
    c, p, w = _xml_escape(cari_kodu), _xml_escape(sifre), _xml_escape(web_siparis_kodu)
    m = op.soap_method
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{m} xmlns="{TEMURI_NS}">
      <CariKodu>{c}</CariKodu>
      <Sifre>{p}</Sifre>
      <WebSiparisKodu>{w}</WebSiparisKodu>
    </{m}>
  </soap:Body>
</soap:Envelope>"""


def _envelope_12(op: Operation, cari_kodu: str, sifre: str, web_siparis_kodu: str) -> str:
    c, p, w = _xml_escape(cari_kodu), _xml_escape(sifre), _xml_escape(web_siparis_kodu)
    m = op.soap_method
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <{m} xmlns="{TEMURI_NS}">
      <CariKodu>{c}</CariKodu>
      <Sifre>{p}</Sifre>
      <WebSiparisKodu>{w}</WebSiparisKodu>
    </{m}>
  </soap12:Body>
</soap12:Envelope>"""


def _extract_result(xml_text: str, result_tag_local: str) -> Optional[str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    want = f"{TEMURI}{result_tag_local}"
    for elem in root.iter():
        if elem.tag == want:
            t = (elem.text or "").strip()
            return t if t else None
    return None


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Surat Kargo — KargoTakipHareketDetayi / Detayli / DetayliV2 (SOAP 1.1 veya 1.2)"
    )
    ap.add_argument(
        "--method",
        choices=tuple(OPERATIONS.keys()),
        default="1",
        help="1=KargoTakipHareketDetayi, 2=KargoTakipHareketDetayli, 3=KargoTakipHareketDetayliV2 (varsayılan: 1)",
    )
    ap.add_argument("--cari-kodu", required=True, help="CariKodu")
    ap.add_argument("--sifre", required=True, help="Sifre")
    ap.add_argument("--web-siparis-kodu", required=True, help="WebSiparisKodu")
    ap.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"Endpoint (varsayılan: {DEFAULT_URL})",
    )
    ap.add_argument("--soap", choices=("1.1", "1.2"), default="1.1")
    ap.add_argument(
        "--raw",
        action="store_true",
        help="Yanıtı ayrıştırmadan ham XML olarak yaz",
    )
    args = ap.parse_args()

    op = OPERATIONS[args.method]
    action = op.soap_action

    if args.soap == "1.1":
        soap_body = _envelope_11(op, args.cari_kodu, args.sifre, args.web_siparis_kodu)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f'"{action}"',
        }
    else:
        soap_body = _envelope_12(op, args.cari_kodu, args.sifre, args.web_siparis_kodu)
        headers = {
            "Content-Type": f'application/soap+xml; charset=utf-8; action="{action}"',
        }

    req = urllib.request.Request(
        args.url,
        data=soap_body.encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print(err_body, file=sys.stderr)
        return e.code if 100 <= e.code < 600 else 1
    except urllib.error.URLError as e:
        print(f"Bağlantı hatası: {e.reason}", file=sys.stderr)
        return 1

    if args.raw:
        print(raw)
        return 0

    result = _extract_result(raw, op.result_tag)
    if result is not None:
        print(result)
        return 0

    try:
        root = ET.fromstring(raw)
        ET.indent(root, space="  ")
        print(ET.tostring(root, encoding="unicode"))
    except ET.ParseError:
        print(raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
