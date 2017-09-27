import { CanvasState } from "../static/pipeline/canvas/drydock";
import { CanvasContextMenu } from "../static/pipeline/canvas/drydock_contextmenu";
import { RawNode } from "../static/pipeline/canvas/drydock_objects";
import * as imagediff from 'imagediff';

describe("Drydock context menu", function() {
    let testMenu;
    let canvasState;
    let $menu;
    let $ul;
    let $li;
    let event;
    let test_action_iterator = 0;

    jasmine.getStyleFixtures().fixturesPath = '/static/pipeline';
    jasmine.getStyleFixtures().preload('./drydock.css');

    beforeAll(function() {
        jasmine.addMatchers(imagediff.jasmine);
        canvasState = new CanvasState(imagediff.createCanvas(300, 150), true);
        let node = new RawNode(50, 50, 'raw');
        canvasState.addShape(node);
        event = new MouseEvent('click', { screenX: 50, screenY: 50 });
        node.doDown(canvasState, event);
    });

    beforeEach(function(){
        appendLoadStyleFixtures('./drydock.css');
        appendSetFixtures("<div id='context_menu'></div>");

        testMenu = new CanvasContextMenu('#context_menu', canvasState);
        $menu = $('#context_menu');
        $ul = $menu.children('ul');
    });

    function registerTestActionThatReturns(criteriaReturn: boolean, action = () => null) {
        testMenu.registerAction(
            "Test action" + test_action_iterator++,
            () => criteriaReturn,
            action
        );
    }

    it("should show", function() {
        registerTestActionThatReturns(true);
        let event = new MouseEvent('click', { screenX: 50, screenY: 50 });
        testMenu.show(event);
        expect($menu).toBeInDOM();
        expect($menu).toBeVisible();
    });

    it("should hide", function() {
        registerTestActionThatReturns(true);
        let event = new MouseEvent('click', { screenX: 50, screenY: 50 });
        testMenu.show(event);
        testMenu.hide();
        expect($menu).toBeHidden();
    });


    it("should open", function() {
        registerTestActionThatReturns(true);
        testMenu.open(event);
        expect($menu).toBeVisible();
    });

    it("should not open when there are no actions", function() {
        testMenu.open(event);
        expect($menu).toBeHidden();
    });

    it("should not open when no registered actions are relevant to the context", function() {
        registerTestActionThatReturns(false);
        testMenu.open(event);
        expect($menu.css('display')).toBe('none');
    });

    it("should cancel", function() {
        registerTestActionThatReturns(true);
        testMenu.open(event);
        testMenu.cancel();
        expect($menu).toBeHidden();
    });

    it("should cancel when user clicks elsewhere", function() {
        registerTestActionThatReturns(true);
        testMenu.open(event);
        $(document).click();
        expect($menu).toBeHidden();
    });

    it("should cancel when user presses escape", function() {
        registerTestActionThatReturns(true);
        testMenu.open(event);
        let escEvent = new KeyboardEvent('keydown', { key: "Escape", bubbles: true, cancelable: true });
        $ul.children('li')[0].dispatchEvent(escEvent);
        expect($menu).toBeHidden();
    });


    it("should register a custom action", function() {
        try {
            registerTestActionThatReturns(true);
        } catch (e) {
            fail(e);
        }
        expect($ul).toContainElement('li');
    });

    it("should enforce unique identifiers for custom actions", function() {
        registerTestActionThatReturns(true);
        test_action_iterator--;
        registerTestActionThatReturns(true);
        expect($ul.children().length).toBe(1);
    });

    it("should only show actions that are relevant to the context", function() {
        registerTestActionThatReturns(false);
        registerTestActionThatReturns(true);
        testMenu.open(event);
        expect($menu.find('li:visible').length).toEqual(1);
    });

    it("should trigger an action when menu item is clicked", function() {
        let spied = { action: () => {} };
        spyOn(spied, 'action');
        registerTestActionThatReturns(true, () => spied.action() );
        $ul.children('li:first').click();
        expect(spied.action).toHaveBeenCalled();
    });

    it("should capture mouse and keyboard events", function() {
        registerTestActionThatReturns(true);
        $li = $ul.children('li');
        var documentClick = spyOnEvent('body', 'click');
        var documentKeyDown = spyOnEvent('body', 'keydown');
        $li[0].dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        $li[0].dispatchEvent(new KeyboardEvent('keydown', { key: "Enter", bubbles: true, cancelable: true }));
        expect(documentClick).not.toHaveBeenTriggered();
        expect(documentKeyDown).not.toHaveBeenTriggered();
    });
});