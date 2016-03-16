$(function() {
    // Security stuff to prevent cross-site scripting.
    noXSS();
    var is_user_admin = false, // Never show admin tools on this page
        dataset_input_table = $('#dataset_input_table tbody'),
        dataset_search_dialog = $('.dataset-search-dlg'),
        set_dataset = {
            wrapper: $('#insert_dataset'),
            btn: $('#insert_one_dataset'),
            options_btn: $('#insert_many_dataset'),
            options_menu: $('#insert_many_menu')
        },
        above_box = $('#above_box'),
        dataset_search_table = new choose_inputs.DatasetsTable(
            dataset_search_dialog.find('table'),
            is_user_admin,
            NaN, NaN,// these will be set later
            dataset_search_dialog.find('.active_filters'),
            dataset_search_dialog.find(".navigation_links")
        ),
        cell_width = 100 / dataset_input_table.find('tr').eq(0).find('td').length + '%'
    ;

    dataset_input_table.find('td').css('width', cell_width);

    above_box.hide = function() {
        this.animate({
            height: '50px',
            'border-color': 'transparent',
            'background-color': 'transparent'
        }).addClass('hidden');
    };
    above_box.show = function(callback) {
        this.animate({
            height: '30em',
            'border-color': '#000',
            'background-color': '#eee'
        }, callback).removeClass('hidden');
    };
    above_box.showIfHidden = function(callback) {
        if (this.is('.hidden')) {
            this.show(callback);
        } else {
            callback();
        }
    };
    dataset_input_table.error = function(message) {
        var $error_div = $(this).closest('table').find('.error');
        $error_div.show().text(message);

        setTimeout(function() {
            $error_div.hide();
        }, 5000);
    };

    var deselectAll = function() {
        dataset_input_table
            .find('.selected').removeClass('selected')
            .find('.remove.ctrl').remove();
    };
    var stopProp = function(e) {
        e.stopPropagation();
    };
    var submitDatasetSearch = function(e) {
        e.preventDefault();
        dataset_search_table.filterSet.addFromForm(this);
    };
    var showInputSearchDlg = (function() {
        var dialog_state = {},
            cellOffsetX,
            cellWidth;

        function moveInputSetDatasetButton() {
            var insertBtnOffsetX = set_dataset.wrapper.offset().left -
                    set_dataset.wrapper.position().left;

            cellOffsetX = $('button.receiving').offset().left;

            // Animate green arrow button
            set_dataset.wrapper
                .animate({
                    width: cellWidth,
                    left: cellOffsetX - insertBtnOffsetX
                }, 150, 'linear');
        }

        // dialog_state will allow the dialog to have disjunct states according to which input is at hand.
        // when a different input is selected, the old input's dialog is saved, and the new one is loaded
        // from memory (or else cleared).
        // defining dialog_state's properties in this way makes them unenumerable and immutable.
        Object.defineProperties(dialog_state, {
            init: {
                enumerable: false,
                value: function() {
                    var this_ = this;
                    $("button[name='input']").each(function() {
                        this_[ $(this).data('input-name') ] = {};
                    });
                }
            },
            save: {
                enumerable: false,
                value: function(input_name) {
                    var dlg = dataset_search_dialog;
                    this[input_name] = {
                        search:  $('input[name="smart"]', dlg).val(),
                        creator: $('#creator').val(),
                        date_added: $('#date_added').val(),
                        date_last_run: $('#date_last_run').val(),
                        table: {
                            page: dataset_search_table.page,
                            filters: $('.search_results .active_filters', dlg).children().detach()
                        }
                    };
                }
            },
            load: {
                enumerable: false,
                value: function(name, compounddatatype_id, input_index) {
                    var state = this[name],
                        dst = dataset_search_table;

                    dataset_search_dialog.find('input[name="smart"]')
                                        .val( state.search        || '' );
                    $('#creator')       .val( state.creator       || '' );
                    $('#date_added')    .val( state.date_added    || '' );
                    $('#date_last_run') .val( state.date_last_run || '' );

                    dst.compounddatatype_id = compounddatatype_id;
                    dst.input_index = input_index;
                    dst.input_name = name;
                    if (state.table !== undefined) {
                        dst.page = state.table.page;
                        dataset_search_dialog.find('.search_results .active_filters')
                            .empty()
                            .append(state.table.filters)
                        ;
                        dst.reloadTable();
                    } else {
                        // default filter set
                        dst.filterSet.add('uploaded'); // includes reloadTable()
                    }

                    dst.$table.removeClass('none-selected-error');
                }
            }
        });

        dialog_state.init();

        return function() {
            var $empty_input = $(this),
                input_name = $empty_input.data('input-name'),
                outgoing_input_name = dataset_search_dialog.fadeIn('fast').find('h2 em').text()
            ;

            cellWidth = $empty_input.outerWidth();

            // Save/load dialog state according to the input
            if (input_name !== outgoing_input_name) {
                if (outgoing_input_name) {
                    dialog_state.save(outgoing_input_name);
                }
                dialog_state.load(
                    input_name,
                    $empty_input.data('cdt'),
                    $empty_input.data('dataset-idx')
                );
            }

            // Corresponding cell in pipeline input matrix
            // Set CSS classes for buttons
            dataset_input_table
                .addClass('inactive')
                .find('.receiving')
                .removeClass('receiving')
                .text('+');

            $empty_input
                .addClass('receiving')
                .text('…');

            dataset_search_dialog.find('h2 em')
                .text(input_name);

            // Move green button before and also after revealing above_box.
            // This allows it to start animating concurrently with above_box,
            // but also moves with the correct final position of above_box.
            moveInputSetDatasetButton();
            above_box.showIfHidden(moveInputSetDatasetButton);
        };
    })();
    var uiFactory = (function() {
        var remove_ctrl = $('<div>').addClass('remove ctrl').text('×'),
            plus_button_cell = $('<td>')
                .css('width', cell_width)
                .append(
                    $('<button>')
                        .attr('name', "input")
                        .addClass('select_dataset')
                        .text('+')
                )
                .addClass('pipeline-input'),
            pipeline_original_row = $('tr', dataset_input_table).eq(0).clone(),
            hidden_input = $('<input type="hidden">'),
            input_dataset = $('<td>').addClass('primary input-dataset pipeline-input').css('width', cell_width)
        ;
        return {
            plusButton: function(data) {
                return plus_button_cell.clone().children('button').data(data);
            },
            plusButtonCell: function(data) {
                return plus_button_cell.clone().children('button').data(data).end();
            },
            pipelineInputRow: function() {
                return pipeline_original_row.clone();
            },
            removeCtrl: function() { return remove_ctrl.clone(); },
            hiddenInput: function(name, value) {
                return hidden_input.clone().attr('name', name).val(value);
            },
            inputDatasetCell: function(name, id, extra_data) {
                return input_dataset.clone()
                    .text(name)
                    .data(extra_data)
                    .data('id', id);
            }
        };
    })();
    var closeSearchDialog = function() {
        var $receiving_button = $('button.receiving'),
            $row = $receiving_button.closest('tr');

        $receiving_button.replaceWith(
            uiFactory.plusButton( $receiving_button.data() )
        );

        dataset_input_table.removeClass('inactive');

        if (
            dataset_input_table.find('tr:not(:has(.input-dataset))').length > 1 &&
            $row.find('.input-dataset').length === 0
        ) {
            $row.remove();
        }

        dataset_search_dialog.fadeOut('fast');
        above_box.hide();
    };
    var initUsersList = function(datasets) {
        var users = [];

        for (var i=0, dataset; (dataset = datasets[i]); i++) {
            if (users.indexOf(dataset.user) == -1) {
                users.push(dataset.user);
            }
        }
        for (i=0; i < users.length; i++) {
            users[i] = $('<option>').attr('value', users[i]).text(users[i]);
        }
        $('#creator').append(users);
    };
    var selectSearchResult = function(e) {
        var $this = $(this),
            $all_trs = $this.parent().find('tr');

        if (e.ctrlKey || e.metaKey) {
            $this.toggleClass('selected');
        } else if (e.shiftKey) {
            var first_selected = $all_trs.filter('.selected').eq(0),
                selected_nextUntil = first_selected.nextUntil($this);

            if (selected_nextUntil.length == first_selected.nextAll().length) {
                first_selected.prevUntil($this).add($this).addClass('selected');
            } else {
                selected_nextUntil.add($this).addClass('selected');
            }
        } else {
            $all_trs.removeClass('selected');
            $this.addClass('selected');
        }
    };
    var addSelectedDatasetsToInput = function(e) {
        var selected_vals = dataset_search_dialog.find('.search_results .selected .primary'),
            receiving_cell = $('button.receiving'),
            receiving_cell_selector = 'td:nth-child(' +
                (receiving_cell.parent().index() + 1) +
                ')',// css pseudo-class is 1-indexed
            receiving_row = receiving_cell.closest('tr'),
            blank_input_queue = receiving_row
                .nextAll().addBack()
                .children(receiving_cell_selector + ':has(button)'),
            inactive_buttons,
            new_row,
            selected_val,
            next_blank_input,
            last_filled_input
        ;

        if (selected_vals.length > 0) {
            dataset_search_table.$table.removeClass('none-selected-error');

            for (var i = 0; i < selected_vals.length; i++) {
                selected_val = selected_vals.eq(i);

                if (blank_input_queue.length === 0) {
                    new_row = uiFactory.pipelineInputRow();
                    new_row.insertAfter(last_filled_input.closest('tr') || receiving_row);

                    // push new row's cell
                    blank_input_queue = blank_input_queue.add(
                        new_row.find(receiving_cell_selector)
                    );
                }

                next_blank_input = blank_input_queue.eq(0);

                last_filled_input = uiFactory.inputDatasetCell(
                    selected_val.text(),
                    selected_val.data('id'),
                    $('button', next_blank_input).data()
                );

                next_blank_input.replaceWith(last_filled_input);

                // shift filled cell out of queue
                blank_input_queue = blank_input_queue.not(next_blank_input);
            }

            inactive_buttons = $('button:not(.receiving)', dataset_input_table);

            // decide where to go next
            if ((e.metaKey || e.ctrlKey) && blank_input_queue.length) {
                blank_input_queue.eq(0).find('button')
                    .trigger('click');
            } else if (inactive_buttons.length) {
                inactive_buttons.eq(0)
                    .trigger('click');
            } else {
                dataset_search_dialog.fadeOut('fast');
                above_box.hide();
            }
        } else {
            dataset_search_table.$table.addClass('none-selected-error');
        }
    };
    var toggleInputDatasetSelection = function(e) {
        var $input_dataset = $(this),
            is_selected = $input_dataset.hasClass('selected');

        deselectAll();

        if (!is_selected) {
            $input_dataset.addClass('selected').prepend( uiFactory.removeCtrl() );
        }
        e.stopPropagation();
    };
    var removeDatasetFromInput = function() {
        var $old_td = $(this).closest('td'),
            $row = $old_td.parent();

        $old_td.replaceWith( $new_td = uiFactory.plusButtonCell( $old_td.data() ) );

        if ($row.find('.input-dataset, .receiving').length === 0) {
            $row.remove();
        }
    };
    var creatorFilterHandler = function() {
        var value = $(this).val();
        dataset_search_table.filterSet.remove('user');
        if (value !== '') {
            dataset_search_table.filterSet.add('user', value);
        }
    };
    var dateAddedFilterHandler = (function() {
        var time, value,
            startOfValue = function() { time.startOf(value); },
            actions = {
                't-0.5h': function() { time.subtract(30, 'minutes'); },
                't-1h':   function() { time.subtract(1, 'hour'); },
                't-1d':   function() { time.subtract(1, 'day'); },
                't-7d':   function() { time.subtract(7, 'day')  .startOf('day'); },
                't-1m':   function() { time.subtract(1, 'month').startOf('day'); },
                't-1y':   function() { time.subtract(1, 'year') .startOf('day'); },
                'day':    startOfValue,
                'week':   startOfValue,
                'month':  startOfValue,
                'year':   startOfValue
            },
            filter_set = dataset_search_table.filterSet
        ;

        return function() {
            time = moment();
            value = $(this).val();
            if (actions.hasOwnProperty(value)) {
                actions[value]();
            } else return;

            filter_set.remove('createdafter');
            filter_set.add(
                'createdafter',
                time.format('DD MMM YYYY HH:mm')
            );
        };
    })();
    var mainSubmitHandler = function(e) {
        var hidden_inputs = [];
        dataset_input_table.find('tr').each(function(run_index) {
            var row = $(this);
            if (row.find('button').length === 0) {
                row.find('.input-dataset').each(function() {
                    var cell = $(this),
                        dataset_id = cell.data('id'),
                        input_index = cell.data('dataset-idx');

                    hidden_inputs.push(
                        uiFactory.hiddenInput(
                            'input_'+ input_index +'['+ run_index +']',
                            dataset_id
                        )
                    );
                });
            } else {
                e.preventDefault();
            }
        });

        if (!e.defaultPrevented) {
            $(this).append(hidden_inputs);
        }
    };
    var focusSearchField = function(e) {
        // prevent this event from bubbling
        if ( $(e.target).is('.search_form') ) {
            $(this).find('input[type="text"]').trigger('focus');
        }
    };
    var addNewRunRow = function() {
        for (
            var new_run_ix = $('tr', dataset_input_table).length;
            $('.run-name[name="run_name[' + new_run_ix + ']"]').length > 0;
            new_run_ix++
        );
        uiFactory.pipelineInputRow()
            .find('.run-name')
                .attr('name', 'run_name[' + new_run_ix + ']')
            .end()
            .appendTo(dataset_input_table)
        ;
        setRunNamesPrefix();
    };
    var removeLastRunRow = function() {
        var $tr = dataset_input_table.find('tr').eq(-1);
        if ($tr.length) {
            if ($tr.find('.receiving').length) {
                closeSearchDialog();
            }
            $tr.eq(-1).remove();
        } else {
            dataset_input_table.error("Error: You must have at least 1 run.");
        }
    };
    var showFillOptions = function() {
        set_dataset.options_menu.show();
        set_dataset.options_btn.addClass('active');
    };
    var hideFillOptions = function() {
        set_dataset.options_menu.hide();
        set_dataset.options_btn.removeClass('active');
    };
    var fillMenuChoose = function(e) {
        var action = $(this).data('action');

        if (action == 'fill-column') {
            // @todo: add pattern fill when multiple datasets are selected
            var selected_val = dataset_search_dialog.find('.search_results .selected .primary').eq(0),
                receiving_cell = $('button.receiving'),
                receiving_cell_selector = 'td:nth-child(' +
                    (receiving_cell.parent().index() + 1) +
                    ')',// css pseudo-class is 1-indexed
                column = receiving_cell
                    .closest('tbody')
                    .children('tr')
                    .children(receiving_cell_selector),
                inactive_buttons
            ;

            if (selected_val.length > 0) {
                dataset_search_table.$table.removeClass('none-selected-error');
                column.replaceWith(
                    uiFactory.inputDatasetCell(
                        selected_val.text(),
                        selected_val.data('id'),
                        receiving_cell.data()
                    )
                );
                inactive_buttons = $('button:not(.receiving)', dataset_input_table);

                // decide where to go next
                if (inactive_buttons.length) {
                    inactive_buttons.eq(0).trigger('click');
                } else {
                    dataset_search_dialog.fadeOut('fast');
                    above_box.hide();
                }
            } else {
                dataset_search_table.$table.addClass('none-selected-error');
            }
        }
    };
    var setRunNamesPrefix = (function() {
        var old_prefix = '';
        return function() {
            var prefix = $('#id_name').val();
            $('.run-name').each(function(ix) {
                var $this = $(this);
                var name_sans_prefix = $this.val().replace(old_prefix +'_', '');
                if (prefix && name_sans_prefix) {
                    $this.val(prefix +'_'+ name_sans_prefix);
                } else if (prefix) {
                    $this.val(prefix +'_'+ ix);
                } else {
                    $this.val(name_sans_prefix);
                }
            });
            old_prefix = prefix;
        };
    })();

    $.fn.textWidth = function(text, font) {
        var this_fn = $.fn.textWidth;
        if (!this_fn.fake_el) {
            this_fn.fake_el = $('<span>').hide().appendTo(document.body);
        }
        this_fn.fake_el
            .text( text || this.val() || this.text() )
            .css( 'font', font || this.css('font') )
        ;
        return this_fn.fake_el.width();
    };
    $.fn.caretTarget = function(offset, start) {
        if (!this.is('input')) return null;
        var position = 0;
        var position_offset = [ 0 ];
        var text = this.val() || this.text();

        // @todo 
        // use the "start" parameter to skip characters.

        // currently just scrolls through until it finds that offset.
        // a better algorithm would do midpoint, then quartiles, etc
        while (position_offset[position] < offset && position < text.length + 1) {
            position++;
            position_offset[position] = this.textWidth(text.substr(0, position));
        }

        return position - 1;
    };

    +function() {
        // override keyboard and mouse events for run name inputs
        // in effect make the prefix portion "read-only" while
        // allowing the user to edit the rest of the name.

        // this closure block exists to close over the following variables.
        var select_start, active_input, $active_input, input_height, input_offset;

        var prefix_el = $('#id_name');

        var selectText = function(e) {// mousemove event when dragging from input
            var prefix_length = prefix_el.val().length + 1,
                full_name_length = active_input.value.length,
                mouse_is_before_input = e.pageY < input_offset.top || 
                    e.pageY < input_height + input_offset.top && 
                    e.pageX < input_offset.left,
                end;

            if (active_input == e.target) {
                end = Math.max(
                    prefix_length,
                    $active_input.caretTarget(e.offsetX, prefix_length)
                );
            } else {
                end = mouse_is_before_input ? prefix_length : full_name_length;
            } 
            if (select_start > end) {
                active_input.setSelectionRange(end, select_start);
            } else {
                active_input.setSelectionRange(select_start, end);
            }
            e.preventDefault();
        };

        $('body').on('mouseup', deactivateInput);

        dataset_input_table.on({// delegate target is ".run-name"
            /* 
             * keydown and mousedown do not provide any information on
             * what's GOING to happen, so we have to reason that ourself
             * based on mouse coordinates and key codes.
             */
            keydown: function(e) {
                var prefix_length = prefix_el.val().length + 1,
                    // these are the keys/combinations we have to watch out for.
                    carat_is_on_boundary = this.selectionStart <= prefix_length,
                    key_is_back_or_left = [8,37].indexOf(e.keyCode) > -1,
                    key_is_up_or_home = [36,38].indexOf(e.keyCode) > -1,
                    select_all_cmd = e.keyCode == 65 && (e.metaKey || e.ctrlKey)
                ;

                if (carat_is_on_boundary && key_is_back_or_left ||
                        key_is_up_or_home || select_all_cmd
                    ) {
                    if (key_is_up_or_home) {
                        this.setSelectionRange(
                            prefix_length, 
                            e.shiftKey ? this.selectionStart : prefix_length
                        );
                    }
                    if (select_all_cmd) {
                        this.setSelectionRange(prefix_length, this.value.length);
                    }
                    e.preventDefault();
                }
            },
            mousedown: function(e) {
                var prefix = prefix_el.val() + '_',
                    offset = e.offsetX,
                    prefix_width = prefix_el.textWidth(prefix);

                activateInput(this);
                if (offset < prefix_width) {
                    this.focus();
                    this.setSelectionRange(prefix.length, prefix.length);
                    e.preventDefault();
                    select_start = prefix.length;
                } else {
                    select_start = $(this).caretTarget(offset, prefix.length);
                }
            }
        }, '.run-name');

        function activateInput(input) {
            active_input = input;
            $active_input = $(input);
            input_offset = $active_input.offset();
            input_height = $active_input.outerHeight();
            $('body').on('mousemove', selectText);
        }
        function deactivateInput() {
            active_input = $active_input = input_offset = input_height = undefined;
            $('body').off('mousemove', selectText);
        }
    }();

    $.getJSON('/api/datasets/?format=json', initUsersList);

    $('body')                  .click(   deselectAll                                            );
    set_dataset.btn            .click(   addSelectedDatasetsToInput                             );
    set_dataset.options_btn    .click(   showFillOptions                                        )
                          .mouseleave(   hideFillOptions                                        );
    $('.permissions-widget')   .click(   stopProp                                               );
    above_box                  .click(   stopProp                                               );
    $('.close.ctrl', above_box).click(   closeSearchDialog                                      );
    $('#date_added')          .change(   dateAddedFilterHandler                                 );
    $('#creator')             .change(   creatorFilterHandler                                   );
    $('#id_name')              .keyup(   setRunNamesPrefix                                      );
    $('#run_pipeline')        .submit(   mainSubmitHandler                                      )
                            .on( 'click',  'input, textarea',      stopProp );
    dataset_search_dialog   .on( 'submit', 'form',                 submitDatasetSearch )
      .find('.search_form') .click(                                focusSearchField );
    dataset_input_table     .on( 'click',  '.input-dataset',       toggleInputDatasetSelection )
                            .on( 'click',  '.remove.ctrl',         removeDatasetFromInput )
                            .on( 'click',  'button[name="input"]', showInputSearchDlg );
    $('.search_results')    .on({ click:                           selectSearchResult,
                                  dblclick:                        function() { set_dataset.btn.click(); }
                            },             'tbody tr' );
    $('#run_controls')      .on( 'click',  '.add_run',             addNewRunRow )
                            .on( 'click',  '.remove_run',          removeLastRunRow );
    set_dataset.options_menu.on( 'click',  'li',                   fillMenuChoose );


    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext">').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });
    
});