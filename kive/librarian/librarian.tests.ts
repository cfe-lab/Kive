require("script-loader!./static/librarian/md5.js");
declare var md5: any;

describe("MD5 library", function() {
    it('should calculate MD5', function() {
        var result = md5('hello');
        expect(result).toBe('5d41402abc4b2a76b9719d911017c592');
    });
});