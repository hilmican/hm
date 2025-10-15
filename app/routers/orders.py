from fastapi import APIRouter

router = APIRouter()


@router.get("")
@router.get("/")
def list_orders():
	return {"orders": []}
