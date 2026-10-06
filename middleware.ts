import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';
import jwt from 'jsonwebtoken';

interface JWTPayload {
  userId: string;
  role: string;
  exp: number;
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow access to /login and all subroutes
  if (pathname.startsWith('/login')) return NextResponse.next();

  // Allow access to static files and API routes
  if (
    pathname.startsWith('/_next') ||
    pathname.startsWith('/api') ||
    pathname === '/favicon.ico' ||
    pathname.startsWith('/public')
  ) {
    return NextResponse.next();
  }

  // Get session token from cookies
  const sessionToken = request.cookies.get('session')?.value;
  if (!sessionToken) {
    const loginUrl = request.nextUrl.clone();
    loginUrl.pathname = '/login';
    return NextResponse.redirect(loginUrl);
  }

  try {
    // If JWT_SECRET is configured, verify token; otherwise allow (DB session check occurs in /api/users/me)
    if (process.env.JWT_SECRET) {
      const payload = jwt.verify(sessionToken, process.env.JWT_SECRET) as JWTPayload;
      if (payload.exp * 1000 < Date.now()) {
        const loginUrl = request.nextUrl.clone();
        loginUrl.pathname = '/login';
        return NextResponse.redirect(loginUrl);
      }

      // Role-based gating for sensitive routes
      const userRole = payload.role;
      if (pathname.startsWith('/admin') || pathname.startsWith('/user-list')) {
        if (userRole !== 'admin' && userRole !== 'super_admin') {
          const loginUrl = request.nextUrl.clone();
          loginUrl.pathname = '/login';
          return NextResponse.redirect(loginUrl);
        }
      }

      const response = NextResponse.next();
      response.headers.set('x-user-id', payload.userId);
      response.headers.set('x-user-role', payload.role);
      return response;
    }

    // If no JWT_SECRET, rely on client-side /api/users/me checks; just allow the request
    return NextResponse.next();
  } catch (error) {
    const loginUrl = request.nextUrl.clone();
    loginUrl.pathname = '/login';
    return NextResponse.redirect(loginUrl);
  }
}

export const config = {
  matcher: ['/((?!api|_next|favicon.ico).*)'],
};
