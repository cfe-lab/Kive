function lock_handler(is_admin) {
    if (is_admin) {
        $('a.redact').show();
    }
    else {
        $('a.redact').hide();
    }
}

$(function(){ // wait for page to finish loading before executing jQuery code
    // Security stuff to prevent cross-site scripting.
    noXSS();
    
    adminLock = new admin_lock.AdminLock(
            $('div.lock'),
            is_user_admin,
            lock_handler);
    lock_handler(false);
});