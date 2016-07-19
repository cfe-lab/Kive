$(function() {
    // Security stuff to prevent cross-site scripting.
    noXSS();

    // "final" vars (should never change)
    // "const" keyword is not well supported otherwise it would be of use here
    var IS_USER_ADMIN = false, // Never show admin tools on this page
        PIPELINE_PK = parseInt($("#id_pipeline").val(), 10);

    // objects (many will be extended below)
    var body = $('body'),
        h1 = $('h1'),
        dataset_input_table = $('#dataset_input_table tbody'),
        dataset_search_dialog = $('.dataset-search-dlg'),
        above_box = $('#above_box'),
        below_box = $('#below_box'),
        scroll_content = $('#scroll_content'),
        set_dataset = {
            wrapper: $('#insert_dataset'),
            btn: $('#insert_one_dataset'),
            options_btn: $('#insert_many_dataset'),
            options_menu: $('#insert_many_menu')
        },
        dataset_search_table = new choose_inputs.DatasetsTable(
            dataset_search_dialog.find('table'),
            IS_USER_ADMIN,
            NaN, NaN,// these will be set later
            dataset_search_dialog.find('.active_filters'),
            dataset_search_dialog.find(".navigation_links")
        )
    ;

    // some more vars
    var cell_width = 100 / dataset_input_table.find('tr').eq(0).find('td').length + '%';
    scroll_content._top = scroll_content.css('top');

    above_box.hide = function() {
        scroll_content.animate({
            'top': scroll_content._top
        }, function() {
            dataset_input_table.$fixed_header.css('top', h1.outerHeight());
        });
        this.animate({
            'height': '50px',
            'border-color': 'transparent',
            'background-color': 'transparent'
        }).addClass('hidden');
    };
    above_box.show = function(callback) {
        var table = dataset_input_table,
            _this = this;
        this.adjustSpacing();
        scroll_content.animate({
            'top': table._top
        }, function() {
            table.$fixed_header.css(
                'top',
                Math.floor(_this.position().top + _this.outerHeight())
            );
        });
        this.animate({
            'height': this._height,
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
    above_box.adjustSpacing = function() {
        if (window.innerHeight <= 700) {
            this._height = "20em";
            dataset_input_table._top = "18em";
        } else if (window.innerHeight <= 1000) {
            this._height = "25em";
            dataset_input_table._top = "23.3em";
        } else {
            this._height = "32em";
            dataset_input_table._top = "31em";
        }
        if (!this.hasClass('hidden')) {
            scroll_content.css('top', dataset_input_table._top);
            this.css('height', this._height);
        }
    };

    dataset_input_table.find('td').css('width', cell_width);
    (function() {// extends dataset_input_table
        function error(message) {
            var $error_div = dataset_input_table.closest('table').find('.error');
            $error_div.show().text(message);
            setTimeout(function() {
                $error_div.hide();
            }, 5000);
        }
        function createHeader() {
            var in_tb = dataset_input_table.closest('table'),
                header_row = in_tb.find('thead tr');
            return in_tb.clone()// <table>
                .wrap('<div>').parent()// <div>
                .addClass('fixed-header')
                .css({
                    height: header_row.outerHeight() + 1,
                    top: h1.outerHeight(),
                    left: in_tb.offset().left,
                    width: in_tb.outerWidth()
                })
                .insertBefore(in_tb)
            ;
        }
        dataset_input_table.scrollHeader = function(e) {
            if (!this.hasOwnProperty("$fixed_header")) {
                this.$fixed_header = createHeader();
            }

            var header = this.$fixed_header,
                header_is_visible = header.is(':visible'),
                header_top = 4;

            if (above_box.hasClass('hidden')) {
                header_top = this.closest('table').offset().top - h1.outerHeight();
            }

            if ($(window).scrollTop() > header_top) {
                if (!header_is_visible) {
                    header.show();
                }
                header.css('left', -$(window).scrollLeft());
            } else if (header_is_visible) {
                header.hide();
            }
        };
        dataset_input_table.addNewRunRow = function() {
            var i, cell_index, new_run_ix, row;
            for (
                new_run_ix = $('tr', this).length;
                $('.run-name[name="run_name[' + new_run_ix + ']"]').length > 0;
                new_run_ix++
            );
            row = uiFactory.pipelineInputRow()
                .find('.run-name')
                    .attr('name', 'run_name[' + new_run_ix + ']')
                .end()
            ;
            if (this.hasOwnProperty('auto_fill')) {
                for (i = 0; (cell_index = this.auto_fill[i]); i++) {
                    row.children('td:nth-child(' + cell_index + ')').replaceWith(
                        this.find('tr:last td:nth-child(' + cell_index + ')').clone(true)//with data and events
                    );
                }
            }
            row.appendTo(this);
            setRunNamesPrefix();
        };
        dataset_input_table.removeLastRunRow = function() {
            var $tr = this.find('tr');
            if ($tr.length > 1) {
                if ($tr.eq(-1).find('.receiving').length) {
                    closeSearchDialog();
                }
                $tr.eq(-1).remove();
            } else {
                error("Error: You must have at least 1 run.");
            }
        };
        dataset_input_table.fillColumn = function(selection, column_ix) {
            // @todo: add pattern fill when multiple datasets are selected
            var selected_vals = selection,
                receiving_cell = $('button.receiving', this),
                inactive_buttons,
                column = receiving_cell
                    .closest('tbody')
                    .children('tr')
                    .children(column_ix)
            ;

            if (selected_vals.length > 0) {
                dataset_search_table.$table.removeClass('none-selected-error');

                column.each(function(ix) {
                    var cell = $(this),
                        selected_val = selected_vals.eq(ix % selected_vals.length);

                    cell.replaceWith(
                        uiFactory.inputDatasetCell(
                            selected_val.text(),
                            selected_val.data('id'),
                            receiving_cell.data()
                        )
                    );
                });
                inactive_buttons = $('button:not(.receiving)', this);

                // decide where to go next
                if (inactive_buttons.length) {
                    inactive_buttons.eq(0).trigger('click');
                } else {
                    dataset_search_dialog.fadeOut('fast');
                    above_box.hide();
                }
                return true;
            } else {
                dataset_search_table.$table.addClass('none-selected-error');
                return false;
            }
        };
        dataset_input_table.deselectAll = function() {
            $('.selected', this).removeClass('selected');
            $('.remove.ctrl', this).remove();
        };
    })();
    var stopProp = function(e) {
        e.stopPropagation();
    };
    var submitDatasetSearch = function(e) {
        e.preventDefault();
        dataset_search_table.filterSet.addFromForm(this);
    };

    (function() {// extends dataset_search_dialog
        var dialog_state = {},
            button = set_dataset.wrapper,
            cellOffsetX,
            cellWidth,
            underset,
            overset,
            d_scroll;

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
        function scrollInputSetDatasetButton() {
            var d_pos = cellOffsetX - button.offset().left - d_scroll;
            if (d_pos) {
                button.css('left', button.position().left + d_pos);
            }
        }
        function moveInputSetDatasetButton() {
            var insertBtnOffsetX = button.offset().left -
                    button.position().left;

            cellOffsetX = $('button.receiving').offset().left;
            underset = cellOffsetX - above_box.offset().left;
            overset = underset + cellWidth - above_box.outerWidth();
            d_scroll = 0;

            if (overset > 0) {
                d_scroll = overset;
            } else if (underset < 0) {
                d_scroll = underset;
            }

            if (d_scroll) {
                $(document).off('scroll');
                body.animate({
                    scrollLeft: body.scrollLeft() + d_scroll
                }, {
                    complete: function() {
                        $(document).on('scroll', scrollInputSetDatasetButton);
                    }
                });
            }
            // Animate green arrow button
            set_dataset.wrapper
                .animate({
                    width: cellWidth,
                    left: cellOffsetX - insertBtnOffsetX - d_scroll
                }, 150, 'linear');
        }
        function scrollToMakeButtonVisible() {
            var receiving_cell = $('button.receiving');
            var button_top = receiving_cell.offset().top;
            underset = button_top - above_box.offset().top - above_box.outerHeight() - 
                dataset_input_table.siblings('thead').outerHeight();
            overset = button_top + receiving_cell.outerHeight() - below_box.offset().top;
            d_scroll = 0;

            if (underset < 0) {
                d_scroll = underset;
            } else if (overset > 0) {
                d_scroll = overset;
            }

            if (d_scroll) {
                body.animate({
                    scrollTop: body.scrollTop() + d_scroll
                });
            }
        }
        function showSearchDialog() {
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
            // moveInputSetDatasetButton();
            above_box.showIfHidden(function() {
                scrollToMakeButtonVisible();
                moveInputSetDatasetButton();
            });
        }
        function closeSearchDialog() {
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
        }
        dialog_state.init();
        dataset_search_dialog.show = showSearchDialog;
        dataset_search_dialog.hide = closeSearchDialog;
        dataset_search_dialog.scrollButton = scrollInputSetDatasetButton;
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
            if ((e.metaKey || e.ctrlKey) && inactive_buttons.length) {
                inactive_buttons.eq(0)
                    .trigger('click');
            } else if (inactive_buttons.length) {
                if (blank_input_queue.length) {
                    blank_input_queue.eq(0).find('button')
                        .trigger('click');
                } else {
                    inactive_buttons.eq(0)
                        .trigger('click');
                }
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

        dataset_input_table.deselectAll();

        if (!is_selected) {
            $input_dataset.addClass('selected').prepend( uiFactory.removeCtrl() );
        }
        e.stopPropagation();
    };
    var removeDatasetFromInput = function() {
        var $old_td = $(this).closest('td'),
            $row = $old_td.parent(),
            auto_fill = dataset_input_table.auto_fill || [],
            auto_fill_index = auto_fill.indexOf($old_td.index() + 1),
            $new_td
        ;

        $old_td.replaceWith( $new_td = uiFactory.plusButtonCell( $old_td.data() ) );

        if ($row.find('.input-dataset, .receiving').length === 0) {
            $row.remove();
        }
        if (auto_fill_index > -1) {
            auto_fill.splice(auto_fill_index, 1);
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
        var serialized_data = serialize(e);

        if (!e.defaultPrevented) {
            console.log(
                $(this).find('input')
                    .not('[name="csrfmiddlewaretoken"]')
                    .not('#id_pipeline')
                    .remove()
            );
            $(this).append(uiFactory.hiddenInput(
                'runbatch',
                JSON.stringify(serialized_data)
            ));
        }

        /**
         * @debug
         */
        console.log(serialized_data);
        e.preventDefault();
        /***/
    };
    var serialize = function(e) {
        var runs = [];
        dataset_input_table.find('tr').each(function(run_index) {
            var run = runs[run_index] = {
                pipeline: PIPELINE_PK,
                description: '',
                users_allowed: [],
                groups_allowed: [],
                inputs: []
            };
            var row = $(this);
            if (row.find('button').length === 0) {
                run.name = row.find('.run-name').val();
                row.find('.input-dataset').each(function() {
                    var cell = $(this);
                    run.inputs.push({
                        index: cell.data('dataset-idx'),
                        dataset: cell.data('id')
                    });
                });
            } else {
                e.preventDefault();
            }
        });
        return {
            name: $('#id_name').val(),
            runs: runs,
            users_allowed: [],
            groups_allowed: [],
            copy_permissions_to_runs: true
        };
    };
    var focusSearchField = function(e) {
        // prevent this event from bubbling
        if ( $(e.target).is('.search_form') ) {
            $(this).find('input[type="text"]').trigger('focus');
        }
    };
    set_dataset.options_menu.show = function() {
        $.fn.show.call(set_dataset.options_menu);//need to call show() from the jQuery object because we're overloading menu.show()
        set_dataset.options_btn.addClass('active');
    };
    set_dataset.options_menu.hide = function() {
        $.fn.hide.call(set_dataset.options_menu);//need to call hide() from the jQuery object because we're overloading menu.hide()
        set_dataset.options_btn.removeClass('active');
    };
    set_dataset.options_menu.choose = function(e) {
        var action = $(this).data('action');

        if (!dataset_input_table.hasOwnProperty('auto_fill')) {
            dataset_input_table.auto_fill = [];
        }

        var selected_val = dataset_search_dialog.find('.search_results .selected .primary'),
            receiving_cell_1index = $('button.receiving').parent().index() + 1,
            receiving_cell_selector = 'td:nth-child(' + receiving_cell_1index + ')',
            auto_fill = dataset_input_table.auto_fill,
            fill_successful
        ;// css pseudo-class is 1-indexed

        if (action == 'fill-column') {
            fill_successful = dataset_input_table.fillColumn(selected_val, receiving_cell_selector);
            if (fill_successful && auto_fill.indexOf(receiving_cell_1index) > -1) {
                auto_fill.splice(auto_fill.indexOf(receiving_cell_1index), 1);
            }
        } else if (action == 'auto-fill-column') {
            fill_successful = dataset_input_table.fillColumn(selected_val, receiving_cell_selector);
            if (fill_successful) {
                auto_fill.push(receiving_cell_1index);
            }
        }
    };
    var setRunNamesPrefix = (function() {
        var old_prefix = '';// closure variable is "static" in effect
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
        var pos = start || 1,
            px_offset = [],
            text = this.val() || this.text(),
            last2_avg, half_char_adjustment;

        // correct offset
        offset += this[0].scrollLeft - parseInt(this.css('padding-left'), 10);

        px_offset[pos - 1] = 0;

        // currently just scrolls through until it finds that offset.
        // a better algorithm would do midpoint, then quartiles, etc
        for (pos; px_offset[pos - 1] < offset && pos - 1 < text.length + 1; pos++) {
            px_offset[pos] = this.textWidth(text.substr(0, pos));
        }
        pos--;

        last2_avg = (px_offset[pos] + px_offset[pos - 1]) / 2;
        half_char_adjustment = +(offset < last2_avg);

        return pos - half_char_adjustment;
    };

    (function() {// handles events for .run_name on dataset_input_table
        // override keyboard and mouse events for run name inputs
        // in effect make the prefix portion "read-only" while
        // allowing the user to edit the rest of the name.

        // this closure block exists to close over the following variables.
        var select_start,
            select_end,
            active_input, 
            $active_input, 
            input_height, 
            input_offset,
            prefix_el = $('#id_name');

        /**
         * keydown and mousedown do not provide any information on
         * what's GOING to happen, so we have to reason that ourselves
         * based on mouse coordinates and key codes.
         */
        var keyDownHandler = function(e) {
            var prefix_length = getPrefix().length,
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
        };
        var getPrefix = function() {
            var prefix = prefix_el.val();
            if (prefix) {
                prefix += "_";
            }
            return prefix;
        };
        var mouseDownHandler = function(e) {
            var prefix = getPrefix(),
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
        };
        var selectText = function(e) {// mousemove event when dragging from input
            var prefix_length = getPrefix().length,
                full_name_length = active_input.value.length,
                mouse_is_before_input = e.pageY < input_offset.top || 
                    e.pageY < input_height + input_offset.top && 
                    e.pageX < input_offset.left;

            if (active_input == e.target) {
                select_end = Math.max(
                    prefix_length,
                    $active_input.caretTarget(e.offsetX, prefix_length)
                );
            } else {
                select_end = mouse_is_before_input ? prefix_length : full_name_length;
            } 
            if (select_start < select_end) {
                active_input.setSelectionRange(select_start, select_end);
            } else {
                active_input.setSelectionRange(select_end, select_start);
            }
            e.preventDefault();
        };
        var selectAll = function(e) {
            e.preventDefault();
            this.setSelectionRange(getPrefix().length, this.value.length);
        };
        var activateInput = function(input) {
            active_input = input;
            $active_input = $(input);
            input_offset = $active_input.offset();
            input_height = $active_input.outerHeight();
            body.on('mousemove', selectText);
        };
        var deactivateInput = function() {
            if (active_input && select_end === undefined) {
                active_input.focus();
                active_input.setSelectionRange(select_start, select_start);
            }
            active_input = $active_input = input_offset = input_height = select_end = undefined;
            $('body').off('mousemove', selectText);
        };

        body.mouseup(deactivateInput);
        dataset_input_table.on({// delegate target is ".run-name"
            keydown: keyDownHandler,
            mousedown: mouseDownHandler,
            dblclick: selectAll
        }, '.run-name');
    })();

    $.getJSON('/api/datasets/?format=json', initUsersList);

    body                    .click(   function() { dataset_input_table.deselectAll(); }   );
    $(document)            .scroll(   dataset_search_dialog.scrollButton                  );
    $(window)              .resize(   dataset_search_dialog.scrollButton                  )
                           .resize(   function() { above_box.adjustSpacing();            })
                           .scroll(   function(e) { dataset_input_table.scrollHeader(e); });
    set_dataset.btn         .click(   addSelectedDatasetsToInput                          );
    set_dataset.options_btn .click(   set_dataset.options_menu.show                         )
                       .mouseleave(   set_dataset.options_menu.hide                         );
    $('.permissions-widget').click(   stopProp                                            );
    above_box               .click(   stopProp                                            )
        .find('.close.ctrl').click(   dataset_search_dialog.hide                          );
    $('#date_added')       .change(   dateAddedFilterHandler                              );
    $('#creator')          .change(   creatorFilterHandler                                );
    $('#id_name')           .keyup(   setRunNamesPrefix                                   );
    $('#run_pipeline')     .submit(   mainSubmitHandler                                   )
                              .on( 'click', 'input, textarea', stopProp                     );
    dataset_search_dialog     .on( 'submit','form',            submitDatasetSearch          )
      .find('.search_form').click(                             focusSearchField             );
    dataset_input_table       .on( 'click', '.input-dataset',  toggleInputDatasetSelection  )
                              .on( 'click', '.remove.ctrl',    removeDatasetFromInput       )
                              .on( 'click', '.select_dataset', dataset_search_dialog.show   );
    $('.search_results')      .on( 'click', 'tbody tr',        selectSearchResult           )
                           .on( 'dblclick', 'tbody tr',        addSelectedDatasetsToInput   );
    set_dataset.options_menu  .on( 'click', 'li',              set_dataset.options_menu.choose );
    $('#run_controls')        .on( 'click', '.add_run',    function() { dataset_input_table.addNewRunRow(); } )
                              .on( 'click', '.remove_run', function() { dataset_input_table.removeLastRunRow(); } );

    // Pack help text into an unobtrusive icon
    $('.helptext', 'form').each(function() {
        var $this = $(this);
        $this.wrapInner('<span class="fulltext">').prepend('<a rel="ctrl">?</a>');
    });

    $('a[rel="ctrl"]').on('click', function (e) {
        $(this).siblings('.fulltext').show().css({ top: e.pageY, left: e.pageX, 'z-index': 3 });
        setTimeout(function() { $('.fulltext').fadeOut(300); }, 5000);
    });

    $(window).scroll();
});