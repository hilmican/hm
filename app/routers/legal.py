from fastapi import APIRouter, Request


router = APIRouter()


@router.get("/legal/privacy")
def privacy(request: Request):
	templates = request.app.state.templates
	return templates.TemplateResponse("privacy.html", {"request": request})


@router.get("/legal/terms")
def terms(request: Request):
	templates = request.app.state.templates
	return templates.TemplateResponse("terms.html", {"request": request})


@router.get("/legal/data-deletion")
def data_deletion(request: Request):
	templates = request.app.state.templates
	return templates.TemplateResponse("data_deletion.html", {"request": request})


