(window as any).$ = (window as any).jQuery = $;
import '@portal/noxss.js';

export class RestApi {

    public static patch(url: string, data, successCallback: JQueryPromiseCallback<any> = () => {},
                        failureCallback: JQueryPromiseCallback<any> = () => {}) {
        return $.ajax({
            type: "PATCH",
            url,
            data,
            dataType: "json"
        })
            .done(successCallback)
            .fail(failureCallback);
    }

    public static put(url: string, data, successCallback: JQueryPromiseCallback<any> = () => {},
                      failureCallback: JQueryPromiseCallback<any> = () => {}) {
        return $.ajax({
            type: "PUT",
            url,
            data,
            contentType: "application/json" // data will not be parsed correctly without this
        })
            .done(successCallback)
            .fail(failureCallback);
    }

    public static post(url: string, data, successCallback: JQueryPromiseCallback<any> = () => {},
                       failureCallback: JQueryPromiseCallback<any> = () => {}) {
        return $.ajax({
            type: "POST",
            url,
            data,
            contentType: "application/json" // data will not be parsed correctly without this
        })
            .done(successCallback)
            .fail(failureCallback);
    }

    public static get(url: string, successCallback: JQueryPromiseCallback<any> = () => {},
                      failureCallback: JQueryPromiseCallback<any> = () => {}) {
        return $.getJSON(url).done(successCallback).fail(failureCallback);
    }

}