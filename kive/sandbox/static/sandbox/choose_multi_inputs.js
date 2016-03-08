$(function() {
    // Security stuff to prevent cross-site scripting.
    noXSS();
    var is_user_admin = false, // Never show admin tools on this page
        dataset_input_table = $('#dataset_input_table tbody'),
        dataset_search_dialog = $('.dataset-search-dlg'),
        input_set_dataset_btn = $('#insert_dataset'),
        above_box = $('#above_box'),
        dataset_search_table = new choose_inputs.DatasetsTable(
            dataset_search_dialog.find('table'),
            is_user_admin,
            NaN, NaN,// these will be set later
            dataset_search_dialog.find('.active_filters'),
            dataset_search_dialog.find(".navigation_links")
        ),
        cell_width = 100/dataset_input_table.find('tr').eq(0).find('td').length + '%'
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
        }, 5000)
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
            var insertBtnOffsetX = input_set_dataset_btn.offset().left -
                    input_set_dataset_btn.position().left;

            // Animate green arrow button
            input_set_dataset_btn
                .animate({
                    width: cellWidth,
                    left: cellOffsetX - insertBtnOffsetX
                }, 150, 'linear');
        }

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

            cellOffsetX = $empty_input.offset().left;
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

            // Move green button before and also after revealing above_box.
            // This allows it to start animating concurrently with above_box,
            // but also moves with the correct final position of above_box.
            moveInputSetDatasetButton();
            above_box.showIfHidden(moveInputSetDatasetButton);

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
            pipelineInputRow: function() { return pipeline_original_row.clone(); },
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
                (receiving_cell.parent().index() + 1)
                + ')',// css pseudo-class is 1-indexed
            receiving_row = receiving_cell.closest('tr'),
            blank_input_queue = receiving_row
                .nextAll().addBack()
                .children(receiving_cell_selector + ':has(button)'),
            inactive_buttons,
            new_row,
            selected_val,
            next_blank_input
        ;

        if (selected_vals.length > 0) {
            dataset_search_table.$table.removeClass('none-selected-error');

            for (var i = 0; i < selected_vals.length; i++) {
                selected_val = selected_vals.eq(i);

                if (blank_input_queue.length === 0) {
                    new_row = uiFactory.pipelineInputRow();
                    new_row.insertAfter(receiving_row)

                    // push new row's cell
                    blank_input_queue = blank_input_queue.add(
                        new_row.find(receiving_cell_selector)
                    );
                }

                next_blank_input = blank_input_queue.eq(0);

                next_blank_input.replaceWith(
                    uiFactory.inputDatasetCell(
                        selected_val.text(),
                        selected_val.data('id'),
                        $('button', next_blank_input).data()
                    )
                );

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
    }
    var addNewRunRow = function() {
        dataset_input_table.append(uiFactory.pipelineInputRow());
    }
    var removeLastRunRow = function() {
        var $trs = dataset_input_table.find('tr')
        if ($trs.length > 1) {
            $trs.eq(-1).remove();
        } else {
            dataset_input_table.error("Error: You must have at least 1 run.");
        }
    }

    $.getJSON('/api/datasets/?format=json', initUsersList);

    above_box             .on( 'click',    '.close.ctrl',           closeSearchDialog           )
                          .on( 'click',                             stopProp                    );
    input_set_dataset_btn .on( 'click',                             addSelectedDatasetsToInput  );
    $('#run_pipeline')    .on( 'submit',                            mainSubmitHandler           )
                          .on( 'click',    'input, textarea',       stopProp                    );
    $('.permissions-widget').on('click',                            stopProp                    );
    dataset_search_dialog .on( 'submit',   'form',                  submitDatasetSearch         )
                          .on( 'change',   '#date_added',           dateAddedFilterHandler      )
                          .on( 'change',   '#creator',              creatorFilterHandler        )
    .find('.search_form') .on( 'click',                             focusSearchField            );
    dataset_input_table   .on( 'click',    '.input-dataset',        toggleInputDatasetSelection )
                          .on( 'click',    '.remove.ctrl',          removeDatasetFromInput      )
                          .on( 'click',    'button[name="input"]',  showInputSearchDlg          );
    $('.search_results')  .on( 'click',    'tbody tr',              selectSearchResult          )
                          .on( 'dblclick', 'tbody tr',              function() { input_set_dataset_btn.click(); } );
    $('body')             .on( 'click',                             deselectAll                 );
    $('#run_controls')    .on( 'click',    '.add_run',              addNewRunRow                )
                          .on( 'click',    '.remove_run',           removeLastRunRow            );

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext"></span>').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });
    
});