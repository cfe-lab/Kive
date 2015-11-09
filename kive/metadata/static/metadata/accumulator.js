$(function() {
  var widget = $('.permissions-widget');
  var css_first = 'pw-selected pw-first-selected';
  initializeWidgetDisplay.call(widget);
  
  $('.pw-users_groups', widget)
    .on('click',    '.pw-option', selectionHandler)
    .on('dblclick', '.pw-option', function() {
      var add_btn = $('.pw-add-one', getWidget(this));
      add_btn.addClass('pw-clicking');
      setTimeout(function() { add_btn.removeClass('pw-clicking'); }, 150);
      addToSelected.call(this);
  });
  $('.pw-users_groups_allowed', widget)
    .on('click',    '.pw-option', selectionHandler)
    .on('dblclick', '.pw-option', function() {
      var remove_btn = $('.pw-remove-one', getWidget(this));
      remove_btn.addClass('pw-clicking');
      setTimeout(function() { remove_btn.removeClass('pw-clicking'); }, 150);
      removeFromSelected.call(this);
  });
  
  $('h5', widget).on('dblclick', function(e) {
    var box = $(this).next('.pw-inner');
    $('.pw-selected', getPanel(box)).removeClass(css_first);
    box.find('.pw-option').addClass('pw-selected');
  });
  
  $('.pw-accumulator-ctrl input', widget)
      .on('mousedown', function() { $(this).addClass('pw-clicking'); })
      .on('mouseup',   function() { $(this).removeClass('pw-clicking'); });
  
  $('.pw-add-one', widget).on('click', addToSelected);
  $('.pw-add-all', widget).on('click', function(e) {
    $('.pw-users_groups .pw-option', getWidget(this)).addClass('pw-selected');
    addToSelected.call(this,e);
  });
  
  $('.pw-remove-one', widget).on('click', removeFromSelected);
  $('.pw-remove-all', widget).on('click', removeAllFromSelected);
  $('.pw-search-box', widget).on('keyup', searchFilter);
  

  function initializeWidgetDisplay() {
    var widget = getWidget(this);
    
    initializeFor('groups');
    initializeFor('users');
    addToSelected.call(this);
    
    function initializeFor(str) {
      /*
      Some browsers (ie. Firefox) allow .val() and :selected to persist on 
      page refresh, so checking the [selected] attr is the only way to see 
      if the -server- specified it was selected.
       */
      var selected = $('.pw-hidden-' + str, widget).val(),
        locked_in = $('.pw-hidden-' + str + ' option[selected]',  widget),
        opts = $('.pw-' + str + '_options .pw-option', widget),
        selectById = labelByIdGenerator(opts, 'pw-selected'),
        lockById   = labelByIdGenerator(opts, 'pw-locked');

      if (selected && selected.length > 0) {
        $.each(selected, selectById);
      }
      if (locked_in.length > 0) {
        $.each(locked_in, lockById);
      }
    }
    function labelByIdGenerator(set_to_select_from, css_class) {
      return function(i, selected_id) {
        if (selected_id instanceof HTMLOptionElement) {
          selected_id = selected_id.value;
        }
        set_to_select_from.filter(function() {
          return $(this).data('value') == selected_id;
        }).addClass(css_class);
      };
    }
  }
  function selectionHandler(e) {
    var $this = $(this),
      panel = getPanel($this),
      first, range;
    
    e.preventDefault();
    if (e.metaKey || e.ctrlKey) {
      $this.toggleClass('pw-selected');
    } else if (e.shiftKey) {
      first = $('.pw-first-selected', panel);
      if (first.length === 0 || first.is(this) || 
            !first.parent().is($this.parent())
            ) {
        selectOnly(this);
      } else {
        // nextUntil will select all siblings -after- `first` until -before- `this`.
        range = first.nextUntil(this);
        // if `this` is not found, nextUntil will select all the next siblings, behaving identical to nextAll.
        // this means that `this` is before `first`, and we have to go backwards instead.
        if (range.length == first.nextAll().length) {
          range = first.prevUntil(this);
        }
        // `this` and `first` must be put back into the set.
        range = range.addBack().add(this);
        $('.pw-selected', panel).removeClass('pw-selected');
        range.addClass('pw-selected');
      }
    } else selectOnly(this);
  }
  function searchFilter(e) {
    var query = new RegExp(escapeRegExp(this.value), 'i'),
      options = $('.pw-users_groups .pw-option', getWidget(this)),
      matched_opts = options.filter(function() {
        return $(this).data('value').toString().search(query) > -1 ||
          $(this).text().search(query) > -1;
      });
    options.not(matched_opts).addClass('pw-search-hidden').removeClass(css_first);
    matched_opts.removeClass('pw-search-hidden');
  }
  function selectOnly(elem) {
    var panel = getPanel(elem);
    $('.pw-selected', panel).removeClass(css_first);
    $(elem).addClass(css_first);
  }
  function addToSelected(e) {
    var widget = getWidget(this);
    var selected = $('.pw-users_groups .pw-selected', widget)
        .not('.pw-search-hidden')
        .removeClass(css_first);
    selected.appendTo($('.pw-users_groups_allowed .pw-inner', widget));
    syncHiddenInput.call(this);
  }
  function removeFromSelected(e) {
    var widget = getWidget(this);
    var selected = $('.pw-users_groups_allowed .pw-selected', widget).removeClass(css_first);
    selected.filter('.pw-group').appendTo($('.pw-groups_options', widget));
    selected.filter('.pw-user' ).appendTo($('.pw-users_options', widget));
    searchFilter.call(widget.find('.pw-search-box')[0]);
    syncHiddenInput.call(this);
  }
  function removeAllFromSelected(e) {
    $('.pw-users_groups_allowed .pw-option', getWidget(this)).addClass('pw-selected');
    removeFromSelected.call(this,e);
  }
  function getWidget (el) {
    return $(el).closest('.permissions-widget');
  }
  function getPanel(elem) {
    return elem.closest('.pw-panel');
  }
  function getOtherPanel(elem) {
    var selector = '.pw-users_groups';
    if (getPanel(elem).is(selector)) {
      selector = '.pw-users_groups_allowed';
    }
    return $(selector, getWidget(this));
  }
  function syncHiddenInput() {
    var widget = getWidget(this);
    var selections = $(".pw-users_groups_allowed .pw-option", widget);
    $('.pw-hidden-users', widget).val(getVals('.pw-user'));
    $('.pw-hidden-groups', widget).val(getVals('.pw-group'));

    function getVals(sel) {
      var ar = [];
      selections.filter(sel).each(function() {
        ar.push($(this).data('value'));
      });
      return ar;
    }
  }
  function escapeRegExp(str) {
    return str.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
  }
});