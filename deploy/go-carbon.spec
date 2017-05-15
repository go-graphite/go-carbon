%define debug_package %{nil}

Name:	    go-carbon
Version:	%{version}
Release:	%{release}%{?dist}
Summary:	Carbon server for graphite

Group:		Development/Tools
License:	MIT License
URL:		https://github.com/lomik/go-carbon
Source0:	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}

%if systemd_requires
%systemd_requires
%else
Requires: initscripts
%endif


%description
Golang implementation of Graphite/Carbon server with classic architecture: Agent -> Cache -> Persister


%prep
rm -rf %{buildroot}
%setup -n %{name}

%build

%pre
%{__id} carbon || /usr/sbin/useradd -U -s /bin/false -c "User for Graphite daemon" carbon

%install
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}
%{__mkdir} -p %{buildroot}%{_bindir}
%{__mkdir} -p %{buildroot}%{_var}/log/%{name}
%{__mkdir} -p %{buildroot}%{_var}/lib/graphite/whisper

%{__install} -pD -m 755 %{name} %{buildroot}/%{_bindir}/%{name}
%{__install} -pD -m 644 %{name}.conf %{buildroot}/%{_sysconfdir}/%{name}/%{name}.conf
%{__install} -pD -m 644 storage-schemas.conf %{buildroot}/%{_sysconfdir}/%{name}/storage-schemas.conf
%{__install} -pD -m 644 storage-aggregation.conf %{buildroot}/%{_sysconfdir}/%{name}/storage-aggregation.conf

%if systemd_requires
%{__install} -pD -m 644 %{name}.service %{buildroot}%{_unitdir}/%{name}.service
%else
%{__install} -pD -m 755 %{name}.init %{buildroot}/%{_initddir}/%{name}
%endif

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%{_bindir}/%{name}

%if systemd_requires
%{_unitdir}/%{name}.service
%else
/etc/init.d/*
%endif

%config(noreplace) %{_sysconfdir}/%{name}/%{name}.conf
%config(noreplace) %{_sysconfdir}/%{name}/storage-schemas.conf
%config(noreplace) %{_sysconfdir}/%{name}/storage-aggregation.conf

%defattr(-,carbon,carbon,-)
%{_var}/log/%{name}
%{_var}/lib/graphite/whisper

%changelog

* Wed Apr 05 2017 Mathieu Grzybek <mathieu@grzybek.fr> 0.10-1 RPM compliance
  systemd-aware build
	create a dedicated user to run the daemon
	create some standard files and directories (conf,log,lib)
