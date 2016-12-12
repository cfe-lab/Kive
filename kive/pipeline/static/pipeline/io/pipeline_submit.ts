import { CanvasState } from "../canvas/drydock";
import { RestApi } from "../rest_api.service";
import { serializePipeline } from "./serializer";

export function buildPipelineSubmit(canvasState: CanvasState, action: string, $family_name: JQuery,
            $family_desc: JQuery, family_pk: number, $revision_name: JQuery, $revision_desc: JQuery,
            parent_revision_id: number, $published: JQuery, $user_permissions: JQuery,
            $group_permissions: JQuery, $error: JQuery, familyNameError: () => any) {

    // $action, $family_pk, and parent_revision_id are static

    if (!(canvasState instanceof CanvasState)) {
        throw "Invalid object given as CanvasState.";
    }
    if (action === 'revise') {
        if (typeof parent_revision_id !== 'number' || isNaN(parent_revision_id) || parent_revision_id < 0) {
            throw 'Parent revision ID must be specified';
        }
    }
    if (action === 'revise' || action === 'add') {
        if (!family_pk && family_pk !== 0) {
            throw 'Family primary key must be specified';
        }
    }
    if ($family_name.length === 0) {
        throw "Family name element could not be found.";
    }
    if ($family_desc.length === 0) {
        throw "Family description element could not be found.";
    }
    if ($revision_name.length === 0) {
        throw "Revision name element could not be found.";
    }
    if ($revision_desc.length === 0) {
        throw "Revision name element could not be found.";
    }
    if ($published.length === 0) {
        throw "Published checkbox element could not be found.";
    }
    if ($published.prop('checked') === undefined) {
        throw "Published checkbox element does not conform to spec.";
    }
    if ($user_permissions.length === 0) {
        throw "User permissions widget could not be found.";
    }
    if ($group_permissions.length === 0) {
        throw "Group permissions widget could not be found.";
    }
    if ($error.length === 0) {
        throw "User error message element could not be found.";
    }
    if (typeof familyNameError !== "function") {
        throw "Error function callback was not supplied or not callable.";
    }

    /*
     * Trigger AJAX transaction on submitting form.
     */
    return function(e) {
        e.preventDefault(); // override form submit action
        clearErrors($error);
        let family = $family_name.val();
        if (action === "new" && family === '') {
            familyNameError();
            submitError('Pipeline family must be named', $error);
        } else {
            try {
                // @todo: action can also be 'add' when pipeline family exists with 0 revisions
                let form_data = serializePipeline(
                    canvasState,
                    {
                        users_allowed: getPermissionsArray($user_permissions),
                        groups_allowed: getPermissionsArray($group_permissions),

                        // There is no PipelineFamily yet; we're going to create one.
                        family,
                        family_desc: $family_desc.val(),

                        // arguments to add first pipeline revision
                        revision_name: $revision_name.val(),
                        revision_desc: $revision_desc.val(),
                        revision_parent: action === 'revise' ? parent_revision_id : null,
                        published: $published.prop('checked'),

                        // Canvas information to store in the Pipeline object.
                        canvas_width: canvasState.width,
                        canvas_height: canvasState.height
                    }
                );

                if (action !== "new") {
                    submitPipelineAjax(family_pk, form_data, $error);
                } else { // Pushing a new family
                    submitPipelineFamilyAjax({
                        users_allowed: form_data.users_allowed,
                        groups_allowed: form_data.groups_allowed,
                        name: form_data.family,
                        description: form_data.family_desc
                    }, $error).done(function (result) {
                        submitPipelineAjax(result.id, form_data, $error);
                    });
                }

            } catch (e) {
                submitError(e, $error);
            }
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
function submitPipelineAjax(family_pk, form_data, $error) {
    return RestApi.post(
        '/api/pipelines/',
        JSON.stringify(form_data),
        function() {
            $(window).off('beforeunload');
            window.location.href = '/pipelines/' + family_pk;
        },
        function (xhr, status, error) {
            let json = xhr.responseJSON;
            let errors = [];

            if (json) {
                if (json.non_field_errors) {
                    submitError(json.non_field_errors, $error);
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
function submitPipelineFamilyAjax(family_form_data, $error) {
    return RestApi.post(
        '/api/pipelinefamilies/',
        JSON.stringify(family_form_data),
        () => {},
        function(xhr, status, error) {
            let json = xhr.responseJSON;
            let serverErrors = json && json.non_field_errors || [];
            if (serverErrors.length === 0) {
                serverErrors = xhr.status + " - " + error;
            }
            submitError(serverErrors, $error);
        }
    );
}
