function tabTheSource($content) {
    // order of precedence for default active tab
    const MODES = {
        'train': 'Train mode',
        'stream': 'Stream mode',
        'mutate': 'Mutate mode',
        'stats': 'Stats mode',
        'write': 'Write mode'
    };

    var $UL = $('<ul class="nav nav-tabs" role="tablist"/>');
    var $LI = $('<li role="presentation"/>');
    var $A = $('<a role="tab" data-toggle="tab" style="text-decoration:none;"/>');
    var $WRAPPER = $('<div class="tab-content content"/>');
    var snippets = [];
    var modeEventElement = {};

    var focusSelectedExample = function(e) {
        var target = $(e.target);
        var beforeTop = target.offset().top - $(window).scrollTop();
        setTimeout(function(){
            var newTop = target.offset().top - beforeTop;
            $('html,body').scrollTop(newTop);
        }, 1);
    };

    var selectTab = function (e) {
        var mode = $(e.target).data('lang');
        var $elements = modeEventElement[mode];
        for (var j = 0; j < $elements.length; j++) {
            $elements[j].tab('show');
        }
    };

    $('div.tabbed-example', $content).each(function () {
        var $exampleBlock = $(this);
        var supportedModes = [];
        var $modeBlocks = {};
        $(this).children('div.tabbed-example-contents').children('div.listingblock,div.informalexample[class*="include-with"]').each(function () {
            var $this = $(this);
            var mode = undefined;
            if ($this.hasClass('listingblock')) {
                mode = $('code', this).data('lang');
            } else {
                for (var key in MODES) {
                    if ($this.hasClass('include-with-' + key)) {
                        mode = key;
                        break;
                    }
                }
            }
            supportedModes.push(mode);
            $modeBlocks[mode] = $(this);
        });

        if (supportedModes.length >= 1) {
            snippets.push({
                '$exampleBlock': $exampleBlock,
                'modes': supportedModes,
                '$modeBlocks': $modeBlocks
            });
        }
    });

    var idNum = 0;
    for (var ix = 0; ix < snippets.length; ix++) {
        var snippet = snippets[ix];
        var supportedModes = snippet.modes;
        var $modeBlocks = snippet.$modeBlocks;
        var $exampleBlock = snippet.$exampleBlock;
        var idBase = 'tabbed-example-' + idNum++;
        var $wrapper = $WRAPPER.clone();
        var $ul = $UL.clone();

        for (var i = 0; i < supportedModes.length; i++) {
            var mode = supportedModes[i];
            var $content = $($modeBlocks[mode]);
            var id;
            if ($content.attr('id')) {
                id = $content.attr('id');
            } else {
                id = idBase + '-' + mode;
                $content.attr('id', id);
            }
            $content.addClass('tab-pane').css('position', 'relative');
            var $li = $LI.clone();
            var $a = $A.clone();

            $a.attr('href', '#' + id).text(MODES[mode]).data('lang', mode).on('shown.bs.tab', selectTab).on('click', focusSelectedExample);

            if (mode in modeEventElement) {
                modeEventElement[mode].push($a);
            } else {
                modeEventElement[mode] = [$a];
            }
            $wrapper.append($content);

            // the first supported mode is active
            // the order is not the order in the doc source, but the order of MODES above
            // but of course it's nice if these are consistent
            if (i === 0) {
                $li.addClass('active');
                $content.addClass('active');
            }

            $li.append($a);
            $ul.append($li);
        }
        $exampleBlock.children('div.example-title', this).first().after($ul);
        $exampleBlock.append($wrapper);
    }
}
