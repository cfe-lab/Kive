import { Pipeline } from "./pipeline_load";
import { CanvasState } from "./drydock";

export class PipelineSubmit {
    
    /**
     * @todo: this class should contain serialize and set/getMetadata
     * functions instead of the Pipeline class.
     */
    
    public static buildSubmit(canvasState: CanvasState, $action: JQuery, $family_name: JQuery,
                $family_desc: JQuery, $family_pk: JQuery, $revision_name: JQuery, $revision_desc: JQuery,
                parent_revision_id, $published: JQuery, $user_permissions: JQuery,
                $group_permissions: JQuery, $error: JQuery, familyNameError: () => any) {
        
        /*
         * Trigger AJAX transaction on submitting form.
         */
        return function(e) {
            e.preventDefault(); // override form submit action
            PipelineSubmit.clearErrors($error);
        
            let action = $action.val();
            let form_data;
            let family = $family_name.val();
    
            if (action == "new" && family === '') {
                familyNameError();
                PipelineSubmit.submitError('Pipeline family must be named', $error);
                return;
            }
            try {
                let pipeline = new Pipeline(canvasState);
                // @todo: check to make sure all arguments exist
                // @todo: action can also be 'add' when pipeline family exists with 0 revisions
                form_data = pipeline.serialize({
                    users_allowed:  PipelineSubmit.getPermissionsArray($user_permissions),
                    groups_allowed: PipelineSubmit.getPermissionsArray($group_permissions),
        
                    // There is no PipelineFamily yet; we're going to create one.
                    family,
                    family_desc: $family_desc.val(),
        
                    // arguments to add first pipeline revision
                    revision_name: $revision_name.val(),
                    revision_desc: $revision_desc.val(),
                    revision_parent: action === 'revise' ? parent_revision_id : null,
                    published: $published.prop('checked'),
        
                    // Canvas information to store in the Pipeline object.
                    canvas_width:  canvasState.width,
                    canvas_height: canvasState.height
                });
            } catch(e) {
                PipelineSubmit.submitError(e, $error);
                return;
            }
        
            if (action !== "new") {
                PipelineSubmit.submitPipelineAjax($family_pk.val(), form_data, $error);
            } else { // Pushing a new family
                PipelineSubmit.submitPipelineFamilyAjax({
                    users_allowed: form_data.users_allowed,
                    groups_allowed: form_data.groups_allowed,
                    name: form_data.family,
                    description: form_data.family_desc
                }, $error).done(function(result) {
                    PipelineSubmit.submitPipelineAjax(result.id, form_data, $error);
                });
            }
        
        };// end exposed function - everything that follows is closed over
    }
    
    private static getPermissionsArray($permissionsElement: JQuery) {
        return $permissionsElement.find("option:selected").get().map(el => el.textContent);
    }
    private static clearErrors($error) {
        $error.empty();
        $('#id_family_name, #id_family_desc, #id_revision_name, #id_revision_desc').removeClass('submit-error-missing');
    }
    private static buildErrors(context, json, errors) {
        for (var field in json) {
            var value = json[field],
                new_context = context;
            if (new_context.length) {
                new_context += ".";
            }
            new_context += field;
            
            for (let i = 0; i < value.length; i++) {
                var item = value[i];
                if (typeof(item) === "string") {
                    errors.push(new_context + ": " + item);
                } else {
                    PipelineSubmit.buildErrors(new_context, item, errors);
                }
            }
        }
    }
    private static submitError(errors, $error) {
        if (errors instanceof Array) {
            $error.empty();
            for (var i = 0; i < errors.length; i++) {
                $error.append($('<p>').text(errors[i]));
            }
        } else {
            $error.text(errors);
        }
        $error.show();
        
        setTimeout(() => $error.hide(), 8000);
    }
    private static submitPipelineAjax(family_pk, form_data, $error) {
        return $.ajax({
            type: "POST",
            url: '/api/pipelines/',
            data: JSON.stringify(form_data),
            contentType: "application/json" // data will not be parsed correctly without this
        }).done(function() {
            $(window).off('beforeunload');
            window.location.href = '/pipelines/' + family_pk;
        }).fail(function(xhr, status, error) {
            var json = xhr.responseJSON,
                errors = [];
            
            if (json) {
                if (json.non_field_errors) {
                    PipelineSubmit.submitError(json.non_field_errors, $error);
                } else {
                    PipelineSubmit.buildErrors("", json, errors);
                    PipelineSubmit.submitError(errors, $error);
                }
            } else {
                PipelineSubmit.submitError(xhr.status + " - " + error, $error);
            }
        });
    }
    private static submitPipelineFamilyAjax(family_form_data, $error) {
        return $.ajax({
            type: "POST",
            url: '/api/pipelinefamilies/',
            data: JSON.stringify(family_form_data),
            contentType: "application/json" // data will not be parsed correctly without this
        }).fail(function(xhr, status, error) {
            var json = xhr.responseJSON,
                serverErrors = json && json.non_field_errors || [];
            
            if (serverErrors.length === 0) {
                serverErrors = xhr.status + " - " + error;
            }
            PipelineSubmit.submitError(serverErrors, $error);
        });
    }
}