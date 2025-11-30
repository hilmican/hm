from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

router = APIRouter(prefix="/soap-test", tags=["soap-test"])


@router.get("", response_class=HTMLResponse)
def soap_test_page(request: Request):
    """Display the SOAP test form"""
    templates = request.app.state.templates
    return templates.TemplateResponse("soap_test.html", {"request": request})


@router.post("/track")
async def track_shipment(
    request: Request,
    gonderen_cari_kodu: str = Form(...),
    takip_no: str = Form(...),
    sifre: str = Form(...),
    soap_version: str = Form("1.1")
):
    """Make SOAP request to track shipment"""
    url = "https://prova.suratkargo.com.tr/services.asmx"
    
    # Escape XML values to prevent injection
    gonderen_cari_kodu_escaped = escape(gonderen_cari_kodu)
    takip_no_escaped = escape(takip_no)
    sifre_escaped = escape(sifre)
    
    if soap_version == "1.1":
        # SOAP 1.1
        soap_action = "http://tempuri.org/TakipNo"
        content_type = "text/xml; charset=utf-8"
        
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <TakipNo xmlns="http://tempuri.org/">
      <GonderenCariKodu>{gonderen_cari_kodu_escaped}</GonderenCariKodu>
      <TakipNo>{takip_no_escaped}</TakipNo>
      <Sifre>{sifre_escaped}</Sifre>
    </TakipNo>
  </soap:Body>
</soap:Envelope>"""
    else:
        # SOAP 1.2
        soap_action = None
        content_type = "application/soap+xml; charset=utf-8"
        
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <TakipNo xmlns="http://tempuri.org/">
      <GonderenCariKodu>{gonderen_cari_kodu_escaped}</GonderenCariKodu>
      <TakipNo>{takip_no_escaped}</TakipNo>
      <Sifre>{sifre_escaped}</Sifre>
    </TakipNo>
  </soap12:Body>
</soap12:Envelope>"""
    
    headers = {
        "Content-Type": content_type,
    }
    if soap_action:
        headers["SOAPAction"] = f'"{soap_action}"'
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, content=soap_body, headers=headers)
            
            # Try to parse the response
            try:
                root = ET.fromstring(response.text)
                # Pretty print XML
                ET.indent(root, space="  ")
                formatted_xml = ET.tostring(root, encoding="unicode")
            except Exception:
                formatted_xml = response.text
            
            return JSONResponse({
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "response_body": formatted_xml,
                "raw_response": response.text,
                "soap_version": soap_version,
                "request_body": soap_body
            })
    except Exception as e:
        return JSONResponse({
            "error": str(e),
            "soap_version": soap_version,
            "request_body": soap_body
        }, status_code=500)

