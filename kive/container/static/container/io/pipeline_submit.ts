import { CanvasState } from "../canvas/drydock";
import { RestApi } from "../rest_api.service";
import { serializePipeline } from "./serializer";

export function buildPipelineSubmit(
    canvasState: CanvasState,
    container_pk: number,
    $error: JQuery) {

    if (!container_pk && container_pk !== 0) {
        throw 'Container primary key must be specified';
    }
    if (!(canvasState instanceof CanvasState)) {
        throw "Invalid object given as CanvasState.";
    }
    if ($error.length === 0) {
        throw "User error message element could not be found.";
    }

    /*
     * Trigger AJAX transaction on submitting form.
     */
    return function(e) {
        e.preventDefault(); // override form submit action
        clearErrors($error);
        try {
            // @todo: action can also be 'add' when pipeline family exists with 0 revisions
            let form_data = serializePipeline(canvasState);

            submitPipelineAjax(container_pk, form_data, $error);

        } catch (e) {
            submitError(e, $error);
        }
    };
}

function getPermissionsArray($permissionsElement: JQuery) {
    return $permissionsElement.find("option:selected").get().map(el => el.textContent);
}
function clearErrors($error) {
    $error.empty();
    $('#id_family_name, #id_family_desc, #id_revision_name, #id_revision_desc').removeClass('submit-error-missing');
}
function buildErrors(context, json, errors) {
    for (let field in json) {
        let value = json[field],
            new_context = context;
        if (new_context.length) {
            new_context += ".";
        }
        new_context += field;

        for (let i = 0; i < value.length; i++) {
            let item = value[i];
            if (typeof(item) === "string") {
                errors.push(new_context + ": " + item);
            } else {
                buildErrors(new_context, item, errors);
            }
        }
    }
}
function submitError(errors, $error) {
    if (Array.isArray(errors)) {
        $error.empty().append(
            errors.map(error => $('<p>').text(error))
        );
    } else {
        $error.text(errors);
    }
    $error.show();
    setTimeout(() => $error.hide(), 8000);
}
function submitPipelineAjax(container_pk, form_data, $error) {
    return RestApi.post(
        '/api/containers/' + container_pk + '/content',
        JSON.stringify(form_data),
        function() {
            $(window).off('beforeunload');
            window.location.href = '/container_update/' + container_pk;
        },
        function (xhr, status, error) {
            let json = xhr.responseJSON;
            let errors = [];

            if (json) {
                if (json.non_field_errors) {
                    submitError(json.non_field_errors, $error);
                } else if (json.detail) {
                    submitError(json.detail, $error);
                } else {
                    buildErrors("", json, errors);
                    submitError(errors, $error);
                }
            } else {
                submitError(xhr.status + " - " + error, $error);
            }
        }
    );
}
