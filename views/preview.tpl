<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>Preview {{collection_id}} | Distillery</title>
    <script>
      function linkify(inputText) {
          var replacedText, replacePattern;
          //URLs starting with http:// or https://
          replacePattern = /(\bhttps?:\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/gim;
          replacedText = inputText.replace(replacePattern, '<a href="$1" target="_blank">$1</a>');
          return replacedText;
      }
      document.addEventListener("DOMContentLoaded", function() {
        // https://web.archive.org/web/20210602172836/https://developer.mozilla.org/en-US/docs/Web/API/EventSource
        const sse = new EventSource("{{base_url}}/distill/{{collection_id}}");
        const eventList = document.querySelector("#log");
        sse.addEventListener("init", function(e) {
          // #cancel and #anchor are moved at first event
          document.body.appendChild(document.querySelector("#init"))
          document.body.appendChild(document.querySelector("#cancel"))
          document.body.appendChild(document.querySelector("#anchor"))
          // #waiting is removed at first event
          document.querySelector("#waiting").remove()
          var newElement = document.createElement("p");
          newElement.textContent = e.data;
          eventList.appendChild(newElement);
        })
        sse.addEventListener("done", function(e) {
          document.getElementById("init").removeAttribute("hidden")
          document.getElementById("init").appendChild(document.getElementById("cancel"))
        })
        sse.addEventListener("message", function(e) {
          // certain messages are displayed inline with span elements
          if (e.data.startsWith(".") || e.data.startsWith("⏳") || e.data.startsWith("☁️")) {
            var newElement = document.createElement("span");
            newElement.textContent = e.data;
            eventList.appendChild(newElement);
          } else {
            var newElement = document.createElement("p");
            newElement.innerHTML = linkify(e.data);
            eventList.appendChild(newElement);
          }
        })
      });
    </script>
    <style type="text/css">
      /* Alignment & Spacing */
      * {
        box-sizing: border-box;
      }
      html, body {
        margin: 0;
        padding: 0;
      }
      button,
      #cancel,
      #log {
        margin: 1em;
      }
      #return {
        padding: 0 1em;
      }
      #waiting {
        align-items: center;
        display: flex;
        flex-direction: column;
        justify-content: center;
        min-height: calc(100vh - 2em);
      }
    </style>
    <style type="text/css">
      /* Text Effects */
      #cancel {
        font-family: sans-serif;
      }
      #log {
        font-family: monospace;
      }
    </style>
    <style type="text/css">
      /* Please Wait Throbber */
      .spinner {
        width: 40px;
        height: 40px;

        position: relative;
        margin: 100px auto;
      }

      .double-bounce1, .double-bounce2 {
        width: 100%;
        height: 100%;
        border-radius: 50%;
        background-color: #333;
        opacity: 0.6;
        position: absolute;
        top: 0;
        left: 0;

        -webkit-animation: sk-bounce 2.0s infinite ease-in-out;
        animation: sk-bounce 2.0s infinite ease-in-out;
      }

      .double-bounce2 {
        -webkit-animation-delay: -1.0s;
        animation-delay: -1.0s;
      }

      @-webkit-keyframes sk-bounce {
        0%, 100% { -webkit-transform: scale(0.0) }
        50% { -webkit-transform: scale(1.0) }
      }

      @keyframes sk-bounce {
        0%, 100% {
          transform: scale(0.0);
          -webkit-transform: scale(0.0);
        } 50% {
          transform: scale(1.0);
          -webkit-transform: scale(1.0);
        }
      }
    </style>
    <style type="text/css">
      /* anchor the bottom of the page once a user scrolls to the bottom */
      body * {
        /* don’t allow the children of the scrollable element to be selected as an anchor node */
        overflow-anchor: none;
      }
      #anchor {
        /* allow the final child to be selected as an anchor node */
        overflow-anchor: auto;
        /* anchor nodes are required to have non-zero area */
        height: 1px;
      }
    </style>
  </head>
  <body>
    <div id="log">
      <div id="waiting">
        <div>preparing preliminary report for {{collection_id}}</em>, please wait (up to a minute)</div>
        <div class="spinner">
          <div class="double-bounce1"></div>
          <div class="double-bounce2"></div>
        </div>
        <!-- document why the following must be within #waiting initially -->
        <form id="init" action="{{base_url}}/distilling" method="post" hidden>
          <input type="hidden" id="collection_id" name="collection_id" value="{{collection_id}}">
          <input type="hidden" id="processes" name="processes" value="{{processes}}">
          <button>Initiate Processing 🚀</button>
        </form>
        <span id="cancel"><a href="{{base_url}}">Cancel</a></span>
        <div id="anchor"></div>
      </div>
    </div>
  </body>
</html>
