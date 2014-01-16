/**
 * Created by art on 2014-Jan-15.
 */
window.onload = function () {
    'use strict';

    var options = document.getElementById('id_ruletype').options;
    var numberOfFields = 0;
    var htmlStr = "";
    document.getElementById('addConstraint').onclick = function() {
        if (numberOfFields < 6) {
            numberOfFields += 1;
            htmlStr = "<table>";
            for (var i = 0; i < numberOfFields; i++) {
                htmlStr += '<tr><th><label for="id_ruletype">Ruletype:</label></th><td><select id="id_ruletype" name="ruletype'+i+'">';
                for (var j = 0; j < options.length; j++) {
                    htmlStr += '<option value="'+options[j].value+'">'+options[j].text+'</option>';
                }
                htmlStr += '</select></th>';
                htmlStr += '<th><label for="id_rule">Rule specification:</label></th><td><input id="id_rule" maxlength="100" name="rule'+i+'" type="text" /></td></tr>';
            }
            htmlStr += "</table>";
            document.getElementById('additionalConstraints').innerHTML = htmlStr;
        }

        //console.log(numberOfFields);
    }
    document.getElementById('removeConstraint').onclick = function() {
        // bind this action
        if (numberOfFields > 0) {
            numberOfFields -= 1;
            htmlStr = "<table>";
            for (var i = 0; i < numberOfFields; i++) {
                htmlStr += '<tr><th><label for="id_ruletype">Ruletype:</label></th><td><select id="id_ruletype" name="ruletype'+i+'">';
                for (var j = 0; j < options.length; j++) {
                    htmlStr += '<option value="'+options[j].value+'">'+options[j].text+'</option>';
                }
                htmlStr += '</select></th>';
                htmlStr += '<th><label for="id_rule">Rule specification:</label></th><td><input id="id_rule" maxlength="100" name="rule'+i+'" type="text" /></td></tr>';
            }
            htmlStr += "</table>";
            document.getElementById('additionalConstraints').innerHTML = htmlStr;
        }
        //console.log(numberOfFields);
    }

}
