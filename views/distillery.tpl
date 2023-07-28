<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
  <title>Distillery</title>
  <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
  <style type="text/css">
    html, body, main { height: 100%; }
    body > main { padding: calc(var(--block-spacing-vertical) / 2) 0; }
    main > footer {
      position: sticky;
      top: 100vh;
    }
    h1 {
      margin-block-start: 0;
      margin-block-end: revert;
    }
    h2 { margin-block: revert; }
    input[type=text] { margin-block-end: revert; }
    :where(input) + small { margin-block-start: revert; }
    details {border-block-end: none; }
    iframe {
      margin-block-end: var(--spacing);
      max-height: 40vh;
      width: 100%;
    }
    #cancel {
      display: flex;
      margin-block-end: var(--spacing);
    }
    #cancel > a {
      align-self: center;
      margin: auto;
    }
    #user [role=button] { white-space: nowrap; }
    @media (min-width: 992px) {
      main > * {
        margin-inline: 17%;
        max-width: 66%;
        padding-inline: var(--spacing);
        width: 100%;
      }
    }
  </style>
</head>

<body>
  <main class="container">
    <header>
      <h1>Distillery</h1>
    </header>
    <hr>
    % if step == "collecting":
    <script>
      function validateCheckboxGroup() {
        var form_data = new FormData(document.querySelector("form"));
        if (!form_data.has("destinations")) {
          var strong = document.createElement("strong");
          strong.textContent = document.querySelector(".required").textContent;
          document.querySelector(".required").innerHTML = strong.outerHTML;
          document.querySelector(".required").style.color = "red";
          document.querySelector(".checkboxes").style.outline = "medium solid red";
          document.querySelector(".checkboxes").style["box-shadow"] = "0 0 0.5rem red";
          return false;
        }
        return true;
      }
    </script>
    <form action="{{distillery_base_url}}" method="post" onsubmit="return validateCheckboxGroup()">
      <div role="group" aria-labelledby="checkbox-group">
        <h2 id="checkbox-group">Destinations</h2>
        <p class="required">Select at least one destination for the available files.</p>
        <div class="checkboxes">
          <label>
            <input type="checkbox" name="destinations" value="cloud">
            Cloud preservation storage
            <small>generate and send files to a remote storage provider</small>
          </label>
          <label>
            <input type="checkbox" name="destinations" value="onsite">
            On-site preservation storage
            <small>generate and send files to a local tape drive</small>
          </label>
          <label>
            <input type="checkbox" name="destinations" value="access">
            Public web access
            <small>generate files & metadata and publish on the web</small>
          </label>
          <fieldset class="publishing" hidden>
            <legend>Choose what happens if digital object file versions exist.</legend>
            <label><input type="radio" name="file_versions_op" value="fail" checked>Fail validation when any digital object file versions exist</label>
            <label><input type="radio" name="file_versions_op" value="overwrite">Overwrite any existing digital object file versions</label>
            <label><input type="radio" name="file_versions_op" value="unpublish">Unpublish any existing digital object file versions</label>
          </fieldset>
          <fieldset class="publishing" hidden>
            <legend>Images with an appended sequence indicator in the filename will be labeled with only the sequence indicator in the thumbnail display.</legend>
            <label><input type="radio" name="thumbnail_label" value="sequence" checked>Use sequence indicator as label</label>
            <label><input type="radio" name="thumbnail_label" value="filename">Use whole filename as label</label>
          </fieldset>
        </div>
      </div>
      <input type="hidden" id="step" name="step" value="validating">
      <button>Validate</button>
      <script>
        const accessCheckbox = document.querySelector('input[value="access"]');
        function handleAccessCheckbox() {
          if (accessCheckbox.checked) {
            document.querySelectorAll(".publishing").forEach(fieldset => fieldset.hidden = false);
          }
        }
        accessCheckbox.addEventListener('change', handleAccessCheckbox);
      </script>
    </form>
    % elif step == "validating":
    <p>Validating metadata, files, and destinations.</p>
    % elif step == "running":
    <p>Processing metadata and files.</p>
    % end
    % if step == "validating" or step == "running":
    <details>
      <summary>Details</summary>
      <iframe src="{{distillery_base_url}}/log"></iframe>
    </details>
    % if step == "validating":
    <form action="{{distillery_base_url}}" method="post">
      <input type="hidden" id="destinations" name="destinations" value="{{destinations}}">
      <input type="hidden" id="step" name="step" value="running">
      <div class="grid">
        <button aria-busy="true" disabled>Validating…</button>
        <div id="cancel"><a href="{{distillery_base_url}}">Cancel</a></div>
      </div>
    </form>
    % elif step == "running":
    <div><a href="{{distillery_base_url}}">back to form</a></div>
    % end
    <script>
      const p = document.getElementsByTagName('p')[0];
      const iframe = document.getElementsByTagName('iframe')[0];
      const button = document.getElementsByTagName('button')[0];
      const details = document.getElementsByTagName('details')[0];
      let id;

      function reloadIframe() {
        iframe.contentDocument.location.reload();
        let text = iframe.contentDocument.body.innerText;
        if (text.indexOf('❌') !== -1) {
          clearInterval(id);
          if (p) {
            p.innerHTML = "❌ Something went wrong. View the details for more information.";
          }
          if (button) {
            button.innerHTML = "❌ Failure";
            button.setAttribute("aria-busy", false);
          }
        } else if (text.indexOf('🈺') != -1 || text.indexOf('🏁') != -1) {
          clearInterval(id);
          if (p) {
            p.innerHTML = p.innerHTML.replace("Validating", "✅ Validated");
            p.innerHTML = p.innerHTML.replace("Processing", "✅ Processed");
          }
          if (button) {
            button.innerHTML = "Run 🚀";
            button.setAttribute("aria-busy", false);
            button.disabled = false;
          }
        }
        iframe.style.height = iframe.contentDocument.body.scrollHeight + 48 + 'px';
        iframe.contentWindow.scrollTo(0, iframe.contentDocument.body.scrollHeight);
      }

      function updateIframe() {
        requestAnimationFrame(reloadIframe);
      }

      details.addEventListener('toggle', updateIframe);
      id = setInterval(updateIframe, 1000);
    </script>
    % end
    <hr>
    <footer>
      <nav id="user">
        <ul>
          <li>Logged in as: {{user["display_name"]}}</li>
        </ul>
        <ul>
          <li><a href="{{distillery_base_url}}/Shibboleth.sso/Logout" role="button" class="secondary outline">Log out</a></li>
        </ul>
      </nav>
    </footer>
  </main>
</body>

</html>
