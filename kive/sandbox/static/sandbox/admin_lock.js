var admin_lock = (function() {
    var my = {};

    /** Create an administrator lock button.
     * 
     * @param $lock_div: jQuery object for the div that will hold the lock
     * button.
     * @param is_user_admin: Is the current user an administrator? If true,
     * will add the lock button to $lock_div, otherwise no change.
     * @param lock_handler: a function that gets called when the lock is
     * clicked. Takes a boolean value: is_admin.
     */
    my.AdminLock = function($lock_div, is_user_admin, lock_handler) {
        if ( ! is_user_admin) {
            $lock_div.hide();
            return;
        }
        
        this.is_admin = false;
        this.lock_handler = lock_handler;
        this.$image = $('<img/>');
        this.$span = $('<span/>');
        
        this.displayLock();
        $lock_div.append(
                $('<a/>').append(this.$image).click(this, toggleLock),
                this.$span)
    }
    
    my.AdminLock.prototype.displayLock = function() {
        this.$image.attr(
                'src',
                this.is_admin
                ? '/static/portal/img/lock-unlocked-2x.png'
                : '/static/portal/img/lock-locked-2x.png');
        this.$span.text(this.is_admin ? 'Administrator:' : '');
    }
    
    toggleLock = function(event) {
        var adminLock = event.data;
        adminLock.is_admin = ! adminLock.is_admin;
        adminLock.displayLock();
        adminLock.lock_handler(adminLock.is_admin);
    }

    return my;
}());
    