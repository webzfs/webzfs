from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, Request

from auth.dependencies import get_current_user
from config.templates import templates
from services.file import read_file, save_file

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/")
def index(request: Request):
    return templates.TemplateResponse(request, name="utils/text/index.jinja")


@router.post("/read")
def read(request: Request, file_path: Annotated[str, Form()]):
    try:
        content = read_file(file_path)
    except Exception as exc:
        return templates.TemplateResponse(
            request, name="partials/error.jinja", context={"error": str(exc)}
        )

    return templates.TemplateResponse(
        request,
        name="utils/text/edit_form.jinja",
        context={"content": content, "file_path": file_path},
    )


@router.post("/save")
def save(
    request: Request, file_path: Annotated[str, Form()], content: Annotated[str, Form()]
):
    context: dict[str, Any] = {"content": content, "file_path": file_path}

    try:
        save_file(file_path, content)
    except Exception as exc:
        context["error"] = str(exc)
    else:
        context["success"] = True

    return templates.TemplateResponse(
        request, name="utils/text/edit_form.jinja", context=context
    )
