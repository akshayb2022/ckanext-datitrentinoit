/**
 * Header Image roll effect
 * Author: Samuele Santi
 * Date: 2012-11-21
 * Time: 11:17 AM
 */

$(document).ready(function(){

    var conf = {
        fadeTime: 800,
        rollTime: 6000
    };

    var sliderBackground = $('.homepage-slider-ng');
    var images = sliderBackground.data('backgrounds');

    var rollImageInit = function(imgroll) {
        imgroll.children().each(function(idx,item){
            if (idx==0) {
                $(item).show();
            }
            else {
                $(item).hide();
            }
        });
    };

    var rollImageNext = function(imgroll, slider) {
        var imgidx = imgroll.data('rollimg-idx') || 0,
            imgcount = imgroll.children().length,
            nextidx = (imgidx + 1) % imgcount,
            oldimg = $(imgroll.children()[imgidx]);
        

        imgroll.children().each(function(idx,item){
            if (idx==imgidx) {
                $(item).hide().css('z-index', '2');
            }
            else if (idx==nextidx) {
                $(item).show().css('z-index', '1');
                slider.css({
                    backgroundImage: images[nextidx]
                });
            }
            else {
                $(item).hide();
            }
        });

        // To allow attaching extra handlers
        $(imgroll).trigger('roll-start', {
            target: $(imgroll.children()[nextidx]),
            conf: conf
        });

        oldimg.fadeOut(conf.fadeTime);
        imgroll.data('rollimg-idx', nextidx);
    };

    $('.homepage-slider-ng .images-wrapper').each(function(){
        var imgroll = $(this);

        rollImageInit(imgroll);
        setInterval(function () {
            rollImageNext(imgroll, sliderBackground);
        }, conf.rollTime);
    });

});
