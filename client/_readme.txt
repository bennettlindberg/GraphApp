#############################################################################

Pre-reqs:
 
  - Install Docker desktop (Mac users be careful with Apple silicon vs.
prior Intel hardware), leave it open/running. Docker desktop is freely
available: https://www.docker.com/products/docker-desktop/

#############################################################################

Linux users (and Windows users running WSL), open a terminal window,
navigate to the folder containing this 'readme.txt' file, and run the 
following command from a terminal window:

  ./_setup-linux.bash

Then build the Docker image by executing this command:

  ./docker/build

Finally, to run the image do the following:

  ./docker/run

If all is well you should see the "docker>" prompt. You only need to do
the setup and build commands once. 

#############################################################################

Mac users: open a terminal window and navigate to the folder containing
this 'readme.txt' file. A nice trick is to open Finder, "View" menu, 
"Show Path Bar", right-click on the folder name, "Open in Terminal". Then
run these commands:

  chmod 755 *.bash

  ./_setup-mac.bash

Now build the Docker image by executing this command:

  ./docker/build

Finally, to run the image do the following:

  ./docker/run

If all is well you should see the "docker>" prompt. You only need to do
the setup and build commands once. 

#############################################################################

Windows users: open a Powershell window and navigate to the folder 
containing this 'readme.txt' file. A nice trick is to view the folder
and right-click on the background and select "Open in Terminal" (if 
Powershell doesn't open, you can change the default profile to Powershell,
save, and try again). Then run this command:

  .\_setup-windows.ps1

If you get an error message along the lines of "script is not digitally 
signed. You cannot run this script on the current system", then run 
as follows:

  powershell.exe -executionpolicy bypass .\_setup-windows.ps1

Now build the Docker image by executing this command:

  ./docker/build

If you get an error message along the lines of "script blocked by system's
execution policy", then do the following and try again:

  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

Finally, to run the image do the following:

  ./docker/run

If all is well you should see the "docker>" prompt. You only need to do
the setup and build commands once. 

#############################################################################

Troubleshooting:

  - If you get "docker not found", then make sure Docker Desktop is
installed and running. Next, uninstall Docker Desktop and reinstall.

  - If you see "you are not authorized", then you may need to login from 
your terminal window:  docker login -u docker-username

  - Other errors? Exit the Terminal/Powershell app and try again. Also, make
sure the folder is NOT hosted in a cloud-syncing service such as DropBox,
OneDrive, or GoogleDrive --- those can trigger errors especially when using
a programming-based IDE along with this folder.

#############################################################################
#############################################################################
