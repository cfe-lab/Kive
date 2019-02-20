import { CanvasState } from "../canvas/drydock";
import { RestApi } from "../rest_api.service";
import { serializePipeline } from "./serializer";
import {PipelineConfig} from "@container/io/PipelineApi";
import {Dialog} from "@container/pipeline_dialogs";

export function buildPipelineSubmit(
    canvasState: CanvasState,
    $container_pk: JQuery,
    $memory: JQuery, // in MB
    $threads: JQuery,
    $error: JQuery,
    $new_tag?: JQuery,
    $new_description?: JQuery,
    $save_as_dialog?: Dialog) {

    if ($container_pk.length === 0) {
        throw 'Container primary key element could not be found.';
    }
    if (!(canvasState instanceof CanvasState)) {
        throw "Invalid object given as CanvasState.";
    }
    if ($memory.length === 0) {
        throw "Memory element could not be found.";
    }
    if ($threads.length === 0) {
        throw "Threads element could not be found.";
    }
    if ($error.length === 0) {
        throw "User error message element could not be found.";
    }
    if ($new_tag !== undefined && $new_tag.length === 0) {
        throw "New tag element could not be found.";
    }
    if ($new_description !== undefined && $new_description.length === 0) {
        throw "New description element could not be found.";
    }

    /*
     * Trigger AJAX transaction on submitting form.
     */
    return function(e) {
        e.preventDefault(); // override form submit action
        clearErrors($error);
        try {
            let form_data = serializePipeline(canvasState);
            let container_pk = parseInt($container_pk.val(), 10),
                memory = parseInt($memory.val(), 10),
                threads = parseInt($threads.val(), 10);

            form_data.pipeline.default_config = <PipelineConfig> {
                memory: memory,
                threads: threads
            };
            if ($new_tag !== undefined) {
                form_data.new_tag = $new_tag.val();
                form_data.new_description = $new_description.val();
            }

            submitPipelineAjax(container_pk, form_data, $error);

        } catch (e) {
            submitError(e, $error);
        }
        if ($save_as_dialog !== undefined) {
            $save_as_dialog.hide();
        }
    };
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
    return RestApi.put(
        '/api/containers/' + container_pk + '/content/',
        JSON.stringify(form_data),
        function(data) {
            $(window).off('beforeunload');
            window.location.href = '/container_update/' + data.id;
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
