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
Requires:  /etc/init.d/functions
%endif


%description
Golang implementation of Graphite/Carbon server with classic architecture: Agent -> Cache -> Persister


%prep
rm -rf %{buildroot}
%setup -n %{name}

%build

%pre
%{__id} _graphite || /usr/sbin/useradd -U -s /bin/false -c "User for Graphite daemon" _graphite

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
%{__install} -pD -m 755 %{name}.init %{buildroot}/etc/init.d/%{name}
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

%defattr(-,_graphite,_graphite,-)
%{_var}/log/%{name}
%{_var}/lib/graphite/whisper

%changelog

* Fri Oct 25 2016 Mathieu Grzybek <vmathieu@grzybek.fr> 0.8.1-1 RPM compliance
  systemd-aware build
	create a dedicated user to run the daemon
	create some standard files and directories (conf,log,lib)
