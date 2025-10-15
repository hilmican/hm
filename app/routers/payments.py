from fastapi import APIRouter

router = APIRouter()


@router.get("")
@router.get("/")
def list_payments():
	return {"payments": []}
