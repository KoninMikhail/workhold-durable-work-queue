// Landing hero background: a drifting node network with tasks travelling along edges.
(function () {
  "use strict";

  var LINK_DISTANCE = 150;
  var stopCurrent = null;

  function start(canvas) {
    var ctx = canvas.getContext("2d");
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var nodes = [];
    var pulses = [];
    var width = 0;
    var height = 0;
    var frame = 0;
    var running = true;

    function resize() {
      var ratio = window.devicePixelRatio || 1;
      width = canvas.clientWidth;
      height = canvas.clientHeight;
      canvas.width = Math.round(width * ratio);
      canvas.height = Math.round(height * ratio);
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      var count = Math.max(24, Math.min(70, Math.round((width * height) / 9000)));
      nodes = [];
      for (var i = 0; i < count; i++) {
        nodes.push({
          x: Math.random() * width,
          y: Math.random() * height,
          vx: (Math.random() - 0.5) * 0.25,
          vy: (Math.random() - 0.5) * 0.25,
        });
      }
      pulses = [];
    }

    function edges() {
      var list = [];
      for (var i = 0; i < nodes.length; i++) {
        for (var j = i + 1; j < nodes.length; j++) {
          var dx = nodes[i].x - nodes[j].x;
          var dy = nodes[i].y - nodes[j].y;
          var d = Math.sqrt(dx * dx + dy * dy);
          if (d < LINK_DISTANCE) list.push([i, j, d]);
        }
      }
      return list;
    }

    function draw() {
      ctx.clearRect(0, 0, width, height);
      var links = edges();
      for (var k = 0; k < links.length; k++) {
        var a = nodes[links[k][0]];
        var b = nodes[links[k][1]];
        ctx.strokeStyle = "rgba(255,255,255," + (0.16 * (1 - links[k][2] / LINK_DISTANCE)).toFixed(3) + ")";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
      ctx.fillStyle = "rgba(255,255,255,0.55)";
      for (var n = 0; n < nodes.length; n++) {
        ctx.fillRect(nodes[n].x - 1, nodes[n].y - 1, 2, 2);
      }
      if (!reduced && links.length && pulses.length < 9 && Math.random() < 0.06) {
        var pick = links[Math.floor(Math.random() * links.length)];
        pulses.push({ from: pick[0], to: pick[1], t: 0 });
      }
      for (var p = pulses.length - 1; p >= 0; p--) {
        var pulse = pulses[p];
        var s = nodes[pulse.from];
        var e = nodes[pulse.to];
        pulse.t += 0.012;
        if (pulse.t >= 1) {
          pulses.splice(p, 1);
          continue;
        }
        var x = s.x + (e.x - s.x) * pulse.t;
        var y = s.y + (e.y - s.y) * pulse.t;
        ctx.fillStyle = "#1a77ff";
        ctx.shadowColor = "#1a77ff";
        ctx.shadowBlur = 10;
        ctx.fillRect(x - 2, y - 2, 4, 4);
        ctx.shadowBlur = 0;
      }
    }

    function step() {
      if (!running) return;
      for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        node.x += node.vx;
        node.y += node.vy;
        if (node.x < 0 || node.x > width) node.vx *= -1;
        if (node.y < 0 || node.y > height) node.vy *= -1;
      }
      draw();
      frame = window.requestAnimationFrame(step);
    }

    resize();
    window.addEventListener("resize", resize);
    if (reduced) {
      draw();
    } else {
      frame = window.requestAnimationFrame(step);
    }

    return function stop() {
      running = false;
      window.cancelAnimationFrame(frame);
      window.removeEventListener("resize", resize);
    };
  }

  function mount() {
    if (stopCurrent) {
      stopCurrent();
      stopCurrent = null;
    }
    var canvas = document.querySelector("canvas[data-wh-net]");
    if (canvas) stopCurrent = start(canvas);
  }

  if (typeof window.document$ !== "undefined") {
    window.document$.subscribe(mount);
  } else {
    document.addEventListener("DOMContentLoaded", mount);
  }
})();
