function tabTheSource($content) {
    var SESSION_STORAGE_KEY = 'active_procedure_mode';
    var MODES = {
        'mutate': 'Mutate mode',
        'stats': 'Stats mode',
        'stream': 'Stream mode',
        'write': 'Write mode'
    };
    updateSelectedModeFromQueryParams(MODES, SESSION_STORAGE_KEY);

    var storedMode = getStoredActiveProcedureMode(SESSION_STORAGE_KEY);

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
        if (storageAvailable('sessionStorage')) {
            sessionStorage.setItem(SESSION_STORAGE_KEY, mode);
        }
    };

    $('div.tabbed-example', $content).each(function () {
        var $exampleBlock = $(this);
        var title = $exampleBlock.children('div.example-title', this).first().text();
        var modes = [];
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
            modes.push(mode);
            $modeBlocks[mode] = $(this);
        });
        if (modes.length > 1) {
            snippets.push({
                '$exampleBlock': $exampleBlock,
                'modes': modes,
                '$modeBlocks': $modeBlocks
            });
        }
    });

    var idNum = 0;
    for (var ix = 0; ix < snippets.length; ix++) {
        var snippet = snippets[ix];
        var modes = snippet.modes;
        var $modeBlocks = snippet.$modeBlocks;
        var $exampleBlock = snippet.$exampleBlock;
        var idBase = 'tabbed-example-' + idNum++;
        var $wrapper = $WRAPPER.clone();
        var $ul = $UL.clone();

        for (var i = 0; i < modes.length; i++) {
            var mode = modes[i];
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

            if (storedMode) {
                if (mode === storedMode) {
                    $li.addClass('active');
                    $content.addClass('active');
                }
            } else if (i === 0) {
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

function storageAvailable(type) {
    try {
        var storage = window[type];
        var x = '__storage_test__';
        storage.setItem(x, x);
        storage.removeItem(x);
        return true;
    }
    catch(e) {
        return false;
    }
}

function getStoredActiveProcedureMode(storageKey) {
    return storageAvailable('sessionStorage') ? sessionStorage.getItem(storageKey) || false : false;
}


function updateSelectedModeFromQueryParams(availableModes, storageKey) {
    var modeFromParams = getQueryParamsFromUrl()["mode"];

    if (modeFromParams && storageAvailable("sessionStorage")) {
        sessionStorage.setItem(storageKey, modeFromParams);
    } else {
        sessionStorage.setItem(storageKey, Object.keys(availableModes)[0]);
    }
}

function getQueryParamsFromUrl() {
    var vars = [];
    var hash = [];
    var hashes = window.location.href
      .split("#")[0]
      .slice(window.location.href.indexOf("?") + 1)
      .split("&");
    for (var i = 0; i < hashes.length; i++) {
        hash = hashes[i].split("=");
        vars.push(hash[0]);
        vars[hash[0]] = hash[1];
    }
    return vars;
}
